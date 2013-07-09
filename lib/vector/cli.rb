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

      if @config[:predictive_scaling][:enabled]
        psconf = @config[:predictive_scaling]

        ps = Vector::Function::PredictiveScaling.new(
          { :cloudwatch => cloudwatch }.merge(psconf))

        groups.each do |group|
          begin
            ps.run_for(group)
          rescue => e
            puts "error performing Predictive Scaling on #{group.name}: #{e.inspect}\n#{e.backtrace.join "\n"}"
          end
        end
      end

      if @config[:flexible_down_scaling][:enabled]
        fdsconf = @config[:flexible_down_scaling]

        fds = Vector::Function::FlexibleDownScaling.new(
          { :cloudwatch => cloudwatch }.merge(fdsconf))

        groups.each do |group|
          fds.run_for(group)
        end
      end
    end

    protected

    def load_config
      opts = {
        :verbose => false,
        :timezone => nil,
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
          :max_sunk_cost => nil
        }
      }

      optparser = OptionParser.new do |o|
        o.banner = "Usage: vector [options]"
        o.separator "DURATION can look like 60s, 1m, 5h, 7d, 1w"

        o.on("--timezone TIMEZONE", "Timezone to use for date calculations (like America/Denver) (default: system timezone)") do |v|
          opts[:timezone] = v
        end

        o.on("--region REGION", "AWS region to operate in (default: us-east-1)") do |v|
          opts[:region] = v
        end

        o.on("--groups group1,group2", Array, "A list of Auto Scaling Groups to evaluate") do |v|
          opts[:groups] = v
        end

        o.on("--fleet fleet", "An AWS ASG Fleet (instead of specifying --groups)") do |v|
          opts[:fleet] = v
        end

        o.on("-v", "--[no-]verbose", "Run verbosely") do |v|
          opts[:verbose] = v
        end

        o.separator ""
        o.separator "Predictive Scaling Options"

        o.on("--[no-]ps", "Enable Predictive Scaling") do |v|
          opts[:predictive_scaling][:enabled] = v
        end

        o.on("--ps-lookback-windows DURATION,DURATION", Array, "List of lookback windows") do |v|
          opts[:predictive_scaling][:lookback_windows] =
            v.map {|w| Vector.time_string_to_seconds(w) }
        end

        o.on("--ps-lookahead-window DURATION", String, "Lookahead window") do |v|
          opts[:predictive_scaling][:lookahead_window] =
            Vector.time_string_to_seconds(v)
        end

        o.on("--ps-valid-threshold FLOAT", Float, "A number from 0.0 - 1.0 specifying how closely previous load must match current load for Predictive Scaling to take effect") do |v|
          opts[:predictive_scaling][:valid_threshold] = v
        end

        o.on("--ps-valid-period DURATION", String, "The period to use when doing the threshold check") do |v|
          opts[:predictive_scaling][:valid_period] = 
            Vector.time_string_to_seconds v
        end

        o.separator ""
        o.separator "Flexible Down Scaling Options"

        o.on("--[no-]fds", "Enable Flexible Down Scaling") do |v|
          opts[:flexible_down_scaling][:enabled] = v
        end

        o.on("--fds-up-to-down DURATION", String, "The cooldown period between up and down scale events") do |v|
          opts[:flexible_down_scaling][:up_down_cooldown] =
            Vector.time_string_to_seconds v
        end

        o.on("--fds-down-to-down DURATION", String, "The cooldown period between down and down scale events") do |v|
          opts[:flexible_down_scaling][:down_down_cooldown] =
            Vector.time_string_to_seconds v
        end

        o.on("--fds-max-sunk-cost DURATION", String, "Only let a scaledown occur if there is an instance this close to its hourly billing point") do |v|
          time = Vector.time_string_to_seconds v
          if time > 1.hour
            puts "--fds-max-sunk-cost duration must be < 1 hour"
            exit 1
          end

          opts[:flexible_down_scaling][:max_sunk_cost] = time
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

      @config = opts
    end
  end
end
