module Vector
  module Function
    class FlexibleDownScaling
      def initialize(options)
        @cloudwatch = options[:cloudwatch]
        @up_down_cooldown = options[:up_down_cooldown]
        @down_down_cooldown = options[:down_down_cooldown]
      end

      def run_for(group)
        # don't check if no config was specified
        return nil if @up_down_cooldown.nil? && @down_down_cooldown.nil?

        # don't bother checking for a scaledown if desired capacity is
        # already at the minimum size...
        return nil if group.desired_capacity == group.min_size

        scaledown_policies = group.scaling_policies.select do |policy|
          policy.scaling_adjustment < 0
        end

        scaledown_policies.each do |policy|
          alarms = policy.alarms.keys.map do |alarm_name|
            @cloudwatch.alarms[alarm_name]
          end

          # only consider disabled alarms (enabled alarms will trigger
          # the policy automatically)
          disabled_alarms = alarms.select do |alarm|
            !alarm.enabled?
          end

          if disabled_alarms.all? {|alarm| alarm.state_value == "ALARM" } &&
              outside_cooldown_period(group)

            # all alarms triggered, and we're outside the cooldown.
            policy.execute

            # no need to evaluate other scaledown policies
            return
          end
        end
      end

      protected

      def outside_cooldown_period(group)
        @cached_outside_cooldown ||= {}
        if @cached_outside_cooldown.has_key? group
          return @cached_outside_cooldown[group]
        end

        activities = previous_scaling_activities(group)
        return nil if activities.nil?

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
        metrics = @cloudwatch.metrics.
          with_namespace("AWS/AutoScaling").
          with_metric_name("GroupDesiredCapacity").
          filter('dimensions', [{
            :name => "AutoScalingGroupName",
            :value => group.name
          }])

        return nil unless metrics.length == 1

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
          if last_value.nil?
            last_value = dp[:average]
          else
            if dp[:average] != last_value
              direction = (last_value < dp[:average]) ? :down : :up
              activities[direction] ||= dp[:timestamp]
            end
          end

          break unless activities.values.any? {|v| v.nil? }
        end

        activities
      end
    end
  end
end
