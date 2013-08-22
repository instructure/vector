require 'optparse'
require 'aws-sdk'
require 'aws/auto_scaling/fleets'
require 'vector/functions/flexible_down_scaling'
require 'vector/functions/predictive_scaling'

module Vector
  class CLI
    def initialize(argv)
      @argv = argv
    end

    def run
      load_config

      auto_scaling = AWS::AutoScaling.new(:region => @config[:region])
      cloudwatch = AWS::CloudWatch.new(:region => @config[:region])

      # everything we do should be fine looking at a snapshot in time,
      # so memoizing should be fine when acting as a CLI.
      AWS.start_memoizing

      groups = if @config[:fleet]
                 auto_scaling.fleets[@config[:fleet]].groups
               else
                 @config[:groups].map do |group_name|
                   auto_scaling.groups[group_name]
                 end
               end

      ps = nil
      if @config[:predictive_scaling][:enabled]
        psconf = @config[:predictive_scaling]
        ps = Vector::Function::PredictiveScaling.new(
          { :cloudwatch => cloudwatch, :dry_run => @config[:dry_run] }.merge(psconf))
      end

      fds = nil
      if @config[:flexible_down_scaling][:enabled]
        fdsconf = @config[:flexible_down_scaling]
        fds = Vector::Function::FlexibleDownScaling.new(
          { :cloudwatch => cloudwatch, :dry_run => @config[:dry_run] }.merge(fdsconf))
      end

      groups.each do |group|
        begin
          ps_check_procs = nil

          if ps
            ps_result = ps.run_for(group)
            ps_check_procs = ps_result[:check_procs]

            if ps_result[:triggered]
              # Don't need to evaluate for scaledown if we triggered a scaleup
              next
            end
          end

          if fds
            fds.run_for(group, ps_check_procs)
          end

        rescue => e
          puts "error for #{group.name}: #{e.inspect}\n#{e.backtrace.join "\n"}"
        end
      end
    end

    protected

    def load_config
      opts = {
        :quiet => false,
        :dry_run => false,
        :region => 'us-east-1',
        :groups => [],
        :fleet => nil,
        :predictive_scaling => {
          :enabled => false,
          :lookback_windows => [],
          :lookahead_window => nil,
          :valid_threshold => nil,
          :valid_period => 60 * 10
        },
        :flexible_down_scaling => {
          :enabled => false,
          :up_down_cooldown => nil,
          :down_down_cooldown => nil,
          :max_sunk_cost => nil,
          :variable_thresholds => false,
          :n_low => nil,
          :n_high => nil,
          :m => nil,
          :g_high => 1.0,
          :g_low => 1.0
        }
      }

      optparser = OptionParser.new do |o|
        o.banner = "Usage: vector [options]"
        o.separator "DURATION can look like 60s, 1m, 5h, 7d, 1w"
        o.set_summary_width 5
        o.set_summary_indent '  '

        def wrap(str)
          str.scan(/\S.{0,#{60}}\S(?=\s|$)|\S+/).join "\n        "
        end

        o.on("--timezone TIMEZONE", wrap("Timezone to use for date calculations (like America/Denver) (default: system timezone)")) do |v|
          Time.zone = v
        end

        o.on("--region REGION", wrap("AWS region to operate in (default: us-east-1)")) do |v|
          opts[:region] = v
        end

        o.on("--groups group1,group2", Array, wrap("A list of Auto Scaling Groups to evaluate")) do |v|
          opts[:groups] = v
        end

        o.on("--fleet fleet", wrap("An AWS ASG Fleet (instead of specifying --groups)")) do |v|
          opts[:fleet] = v
        end

        o.on("--[no-]dry-run", wrap("Don't actually trigger any policies")) do |v|
          opts[:dry_run] = v
        end

        o.on("-q", "--[no-]quiet", wrap("Run quietly")) do |v|
          opts[:quiet] = v
        end

        o.separator ""
        o.separator "Predictive Scaling Options"

        o.on("--[no-]ps", wrap("Enable Predictive Scaling")) do |v|
          opts[:predictive_scaling][:enabled] = v
        end

        o.on("--ps-lookback-windows DURATION,DURATION", Array, wrap("List of lookback windows")) do |v|
          opts[:predictive_scaling][:lookback_windows] =
            v.map {|w| Vector.time_string_to_seconds(w) }
        end

        o.on("--ps-lookahead-window DURATION", String, wrap("Lookahead window")) do |v|
          opts[:predictive_scaling][:lookahead_window] =
            Vector.time_string_to_seconds(v)
        end

        o.on("--ps-valid-threshold FLOAT", Float, wrap("A number from 0.0 - 1.0 specifying how closely previous load must match current load for Predictive Scaling to take effect")) do |v|
          opts[:predictive_scaling][:valid_threshold] = v
        end

        o.on("--ps-valid-period DURATION", String, wrap("The period to use when doing the threshold check")) do |v|
          opts[:predictive_scaling][:valid_period] = 
            Vector.time_string_to_seconds v
        end

        o.separator ""
        o.separator "Flexible Down Scaling Options"

        o.on("--[no-]fds", wrap("Enable Flexible Down Scaling")) do |v|
          opts[:flexible_down_scaling][:enabled] = v
        end

        o.on("--fds-up-to-down DURATION", String, wrap("The cooldown period between up and down scale events")) do |v|
          opts[:flexible_down_scaling][:up_down_cooldown] =
            Vector.time_string_to_seconds v
        end

        o.on("--fds-down-to-down DURATION", String, wrap("The cooldown period between down and down scale events")) do |v|
          opts[:flexible_down_scaling][:down_down_cooldown] =
            Vector.time_string_to_seconds v
        end

        o.on("--fds-max-sunk-cost DURATION", String, wrap("Only let a scaledown occur if there is an instance this close to its hourly billing point")) do |v|
          time = Vector.time_string_to_seconds v
          if time > 1.hour
            puts "--fds-max-sunk-cost duration must be < 1 hour"
            exit 1
          end

          opts[:flexible_down_scaling][:max_sunk_cost] = time
        end

        o.separator ""
        o.on("--[no-]fds-variable-thresholds", wrap("Enable Variable Thresholds")) do |v|
          opts[:flexible_down_scaling][:variable_thresholds] = v
        end

        o.on("--fds-n-low NUM", Integer, wrap("Number of nodes corresponding to --fds-g-low. (default: 1 more than the group's minimum size)")) do |v|
          opts[:flexible_down_scaling][:n_low] = v
        end

        o.on("--fds-n-high NUM", Integer, wrap("Number of nodes corresponding to --fds-g-high. (default: the group's maximum size)")) do |v|
          opts[:flexible_down_scaling][:n_high] = v
        end

        o.on("--fds-m PERCENTAGE", Float, wrap("Maximum target utilization. Will default to the CPUUtilization alarm threshold.")) do |v|
          opts[:flexible_down_scaling][:m] = v / 100
        end

        o.on("--fds-g-high PERCENTAGE", Float, wrap("Capacity headroom to apply when scaling down from --fds-n-high nodes, as a percentage. e.g. if this is 90%, then will not scale down from --fds-n-high nodes until expected utilization on the remaining nodes is at or below 90% of --fds-m. (default: 100)")) do |v|
          opts[:flexible_down_scaling][:g_high] = v / 100
        end

        o.on("--fds-g-low PERCENTAGE", Float, wrap("Capacity headroom to apply when scaling down from --fds-n-low nodes, as a percentage. e.g. if this is 75%, then will not scale down from --fds-n-low nodes until expected utilization on the remaining nodes is at or below 75% of --fds-m. When scaling down from a number of nodes other than --fds-n-high or --fds-n-low, will use a capacity headroom linearly interpolated from --fds-g-high and --fds-g-low. (default: 100)")) do |v|
          opts[:flexible_down_scaling][:g_low] = v / 100
        end

        o.on("--fds-print-variable-thresholds", wrap("Calculates and displays the thresholds that will be used for each asg, and does not execute any downscaling policies. (For debugging).")) do |v|
          opts[:flexible_down_scaling][:print_variable_thresholds] = true
        end

      end.parse!(@argv)

      if opts[:groups].empty? && opts[:fleet].nil?
        puts "No groups were specified."
        exit 1
      end

      if !opts[:groups].empty? && !opts[:fleet].nil?
        puts "You can't specify --groups and --fleet."
        exit 1
      end

      if opts[:predictive_scaling][:enabled]
        ps = opts[:predictive_scaling]
        if ps[:lookback_windows].empty? || ps[:lookahead_window].nil?
          puts "You must specify lookback windows and a lookahead window for Predictive Scaling."
          exit 1
        end
      end

      if opts[:flexible_down_scaling][:enabled]
        fds = opts[:flexible_down_scaling]
        if fds[:up_down_cooldown].nil? ||
           fds[:down_down_cooldown].nil?
          puts "You must specify both up-to-down and down-to-down cooldown periods for Flexible Down Scaling."
          exit 1
        end
      end

      Vector::HLogger.enable(!opts[:quiet])

      @config = opts
    end
  end
end
