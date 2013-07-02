module Vector
  module Function
    class FlexibleDownScaling
      def initialize(options)
        @cloudwatch = AWS::CloudWatch.new

        @up_down_cooldown = Vector.time_string_to_seconds options['up-to-down-cooldown']
        @down_down_cooldown = Vector.time_string_to_seconds options['down-to-down-cooldown']
      end

      def run_for(group)
        # don't check if no config was specified
        return nil if @up_down_cooldown.nil? && @down_down_cooldown.nil?

        # don't bother checking for a scaledown if desired capacity is
        # already at the minimum size...
        return nil if group.desired_capacity == group.min_size

        group.scaling_policies.each do |policy|
          # only evaluate scaling *down* policies
          next if policy.scaling_adjustment >= 0

          policy.alarms.keys.each do |alarm_name|
            alarm = @cloudwatch.alarms[alarm_name]

            # skip alarms that have actions enabled, because they will
            # have already triggered the scaledown
            next if alarm.enabled?

            # if the alarm isn't in the ALARM state, then we don't need
            # to go any further because we won't attempt to scaledown
            # anyway
            next if alarm.state_value != "ALARM"

            # now check that we're outside required cooldown periods
            activities = previous_scaling_activities(group)

            # don't do anything if there was an issue finding activities
            next if activities.nil?

            # check up-down
            if @up_down_cooldown && activities[:up] &&
              Time.now - activities[:up] < @up_down_cooldown
              next
            end

            # check down-down
            if @down_down_cooldown && activities[:down] &&
              Time.now - activities[:down] < @down_down_cooldown
              next
            end

            # ok we're outside cooldown windows, trigger the policy!!
            policy.execute
          end
        end
      end

      protected

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
