require 'vector'

module Vector
  module Function
    class FlexibleDownScaling
      include Vector::HLogger

      def initialize(options)
        @cloudwatch = options[:cloudwatch]
        @dry_run = options[:dry_run]
        @up_down_cooldown = options[:up_down_cooldown]
        @down_down_cooldown = options[:down_down_cooldown]
        @max_sunk_cost = options[:max_sunk_cost]
        @variable_thresholds = options[:variable_thresholds]
        @n_low = options[:n_low]
        @n_high = options[:n_high]
        @m = options[:m]
        @g_low = options[:g_low]
        @g_high = options[:g_high]
        @debug_variable_thresholds = options[:print_variable_thresholds]
      end

      def run_for(group, ps_check_procs)
        result = { :triggered => false }

        hlog_ctx("fds") do
          hlog_ctx("group:#{group.name}") do
            # don't check if no config was specified
            if @up_down_cooldown.nil? && @down_down_cooldown.nil?
              hlog("No cooldown periods specified, exiting")
              return result
            end

            # don't bother checking for a scaledown if desired capacity is
            # already at the minimum size...
            if group.desired_capacity == group.min_size
              hlog("Group is already at minimum size, exiting")
              return result
            end

            scaledown_policies = group.scaling_policies.select do |policy|
              policy.scaling_adjustment < 0
            end

            scaledown_policies.each do |policy|
              hlog_ctx("policy:#{policy.name}") do
                # TODO: support adjustment types other than ChangeInCapacity here
                if policy.adjustment_type == "ChangeInCapacity" &&
                   ps_check_procs &&
                   ps_check_procs.any? {|ps_check_proc|
                     ps_check_proc.call(group.desired_capacity + policy.scaling_adjustment, self) }
                  hlog("Predictive scaleup would trigger a scaleup if group were shrunk")
                  next
                end

                alarms = policy.alarms.keys.map do |alarm_name|
                  @cloudwatch.alarms[alarm_name]
                end

                # only consider disabled alarms (enabled alarms will trigger
                # the policy automatically)
                disabled_alarms = alarms.select do |alarm|
                  !alarm.enabled?
                end

                # Do this logic first in case the user is just trying to print out
                # the thresholds.
                if @variable_thresholds
                  # variable_thresholds currently requires a CPUUtilization alarm to function
                  vt_cpu_alarm = disabled_alarms.find {|alarm| alarm.metric_name == "CPUUtilization" }

                  # remove this alarm from the check, since we're not checking its alarm status
                  # below.
                  disabled_alarms.delete(vt_cpu_alarm)

                  unless vt_cpu_alarm
                    hlog("Variable thresholds requires an alarm on CPUUtilization, skipping")
                    next
                  end

                  @n_low ||= group.min_size + 1
                  @n_high ||= group.max_size
                  @m ||= vt_cpu_alarm.threshold / 100

                  if @g_low == @g_high
                    hlog("g_low == g_high (#{@g_low}), not attempting to use flexible thresholds.")
                    next
                  end

                  if @n_low == @n_high
                    hlog("n_low == n_high (#{@n_low}), not attempting to use flexible thresholds.")
                    next
                  end

                  if @debug_variable_thresholds
                    puts "  n_low: #{@n_low}"
                    puts " n_high: #{@n_high}"
                    puts "      m: #{@m}"
                    puts "  g_low: #{@g_low}"
                    puts " g_high: #{@g_high}"
                    puts
                    puts "  N  Threshold"
                    ([@n_low, group.min_size].min + 1).upto([@n_high, group.max_size].max) do |i|
                      puts " %2d  %.1f%%" % [i, (variable_threshold(i, @n_low, @n_high, @m, @g_low, @g_high) * 100)]
                    end
                    next
                  end
                end

                unless disabled_alarms.all? {|alarm| alarm.state_value == "ALARM" }
                  hlog("Not all alarms are in ALARM state")
                  next
                end

                if @variable_thresholds
                  threshold = variable_threshold(group.desired_capacity, @n_low, @n_high, @m, @g_low, @g_high)

                  stats = vt_cpu_alarm.metric.statistics(
                    :start_time => Time.now - (vt_cpu_alarm.period * vt_cpu_alarm.evaluation_periods),
                    :end_time => Time.now,
                    :statistics => [ vt_cpu_alarm.statistic ],
                    :period => vt_cpu_alarm.period)

                  if stats.datapoints.length < vt_cpu_alarm.evaluation_periods
                    hlog("Could not get enough datapoints for checking variable threshold");
                    next
                  end

                  if stats.datapoints.any? {|dp| dp[vt_cpu_alarm.statistic.downcase.to_sym] > (threshold * 100) }
                    hlog("Not all datapoints are beneath the variable threshold #{(threshold * 100).to_i}: #{stats.datapoints}")
                    next
                  end

                  hlog("Variable threshold: #{(threshold * 100).to_i}, #{group.desired_capacity} nodes")
                end

                unless outside_cooldown_period(group)
                  hlog("Group is not outside the specified cooldown periods")
                  next
                end

                unless has_eligible_scaledown_instance(group)
                  hlog("Group does not have an instance eligible for scaledown due to max_sunk_cost")
                  next
                end

                if @dry_run
                  hlog("Executing policy (DRY RUN)")
                else
                  hlog("Executing policy")
                  policy.execute(:honor_cooldown => true)
                end

                result[:triggered] = true

                # no need to evaluate other scaledown policies
                return result
              end
            end
          end
        end

        result
      end

      protected

      def variable_threshold(n, n_low, n_high, m, g_low, g_high)
        m_high = g_high * m
        m_low = g_low * m
        a = (m_high - m_low).to_f / (n_high - n_low).to_f
        b = m_low - (n_low * a)
        res = (a * n + b) * (1.0 - (1.0 / n))
        res
      end

      def has_eligible_scaledown_instance(group)
        return true if @max_sunk_cost.nil?

        group.ec2_instances.select {|i| i.status == :running }.each do |instance|
          # get amount of time until hitting the instance renewal time
          time_left = ((instance.launch_time.min - Time.now.min) % 60).minutes

          # if we're within 1 minute, assume we won't be able to terminate it
          # in time anyway and ignore it.
          if time_left > 1.minute and time_left < @max_sunk_cost
            # we only care if there is at least one instance within the window
            # where we can scale down
            return true
          end
        end

        false
      end

      def outside_cooldown_period(group)
        @cached_outside_cooldown ||= {}
        if @cached_outside_cooldown.has_key? group
          return @cached_outside_cooldown[group]
        end

        activities = previous_scaling_activities(group)
        return nil if activities.nil?

        if activities[:up]
          hlog "Last scale up #{(Time.now - activities[:up]).minutes.inspect} ago"
        end
        if activities[:down]
          hlog "Last scale down #{(Time.now - activities[:down]).minutes.inspect} ago"
        end
        result = true

        # check up-down
        if @up_down_cooldown && activities[:up] &&
          Time.now - activities[:up] < @up_down_cooldown
          result = false
        end

        # check down-down
        if @down_down_cooldown && activities[:down] &&
          Time.now - activities[:down] < @down_down_cooldown
          result = false
        end

        result
      end

      # Looks at the GroupDesiredCapacity metric for the specified
      # group, and finds the most recent change in value.
      #
      # @returns
      #   * nil if there was a problem getting data. There may have been
      #     scaling events or not, we don't know.
      #   * a hash with two keys, :up and :down, with values indicating
      #     when the last corresponding activity happened. If the
      #     activity was not seen in the examined time period, the value
      #     is nil.
      def previous_scaling_activities(group)
        metric = @cloudwatch.metrics.
          with_namespace("AWS/AutoScaling").
          with_metric_name("GroupDesiredCapacity").
          filter('dimensions', [{
            :name => "AutoScalingGroupName",
            :value => group.name
          }]).first

        return nil unless metric

        start_time = Time.now - [ @up_down_cooldown, @down_down_cooldown ].max
        end_time = Time.now

        stats = metric.statistics(
          :start_time => start_time,
          :end_time => end_time,
          :statistics => [ "Average" ],
          :period => 60)

        # check if we got enough datapoints... if we didn't, we need to
        # assume bad data and inform the caller. this code is basically
        # checking if the # of received datapoints is within 50% of the
        # expected datapoints.
        got_datapoints = stats.datapoints.length
        requested_datapoints = (end_time - start_time) / 60
        if !Vector.within_threshold(0.5, got_datapoints, requested_datapoints)
          return nil
        end

        # iterate over the datapoints in reverse, looking for the first
        # change in value, which should be the most recent scaling
        # activity
        activities = { :down => nil, :up => nil }
        last_value = nil
        stats.datapoints.sort {|a,b| b[:timestamp] <=> a[:timestamp] }.each do |dp|
          next if dp[:average].nil?

          unless last_value.nil?
            if dp[:average] != last_value
              direction = (last_value < dp[:average]) ? :down : :up
              activities[direction] ||= dp[:timestamp]
            end
          end

          last_value = dp[:average]
          break unless activities.values.any? {|v| v.nil? }
        end

        activities
      end
    end
  end
end
