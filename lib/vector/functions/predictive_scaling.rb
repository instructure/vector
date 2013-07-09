module Vector
  module Function
    class PredictiveScaling
      include Vector::HLogger

      def initialize(options)
        @cloudwatch = options[:cloudwatch]
        @lookback_windows = options[:lookback_windows]
        @lookahead_window = options[:lookahead_window]
        @valid_threshold = options[:valid_threshold]
        @valid_period = options[:valid_period]
      end

      def run_for(group)
        hlog_ctx "group: #{group.name}" do
          return if @lookback_windows.length == 0

          scaleup_policies = group.scaling_policies.select do |policy|
            policy.scaling_adjustment > 0
          end

          scaleup_policies.each do |policy|
            hlog_ctx "policy: #{policy.name}" do

              policy.alarms.keys.each do |alarm_name|
                alarm = @cloudwatch.alarms[alarm_name]
                hlog_ctx "alarm: #{alarm.name} (metric #{alarm.metric.name})" do

                  unless alarm.enabled?
                    hlog "Skipping disabled alarm"
                    next
                  end

                  # Note that everywhere we say "load" what we mean is
                  # "metric value * number of nodes"
                  now_load, now_num = load_for(group, alarm.metric,
                    Time.now, @valid_period)

                  if now_load.nil?
                    hlog "Could not get current total for metric"
                    next
                  end

                  @lookback_windows.each do |window|
                    hlog_ctx "window: #{window.inspect}" do
                      then_load, = load_for(group, alarm.metric,
                        Time.now - window, @valid_period)

                      if then_load.nil?
                        hlog "Could not get past total value for metric"
                        next
                      end

                      # check that the past total utilization is within
                      # threshold% of the current total utilization
                      if @valid_threshold &&
                        !Vector.within_threshold(@valid_threshold, now_load, then_load)
                        hlog "Past metric total value not within threshold (current #{now_load}, then #{then_load})"
                        next
                      end

                      past_load, = load_for(group, alarm.metric,
                        Time.now - window + @lookahead_window,
                        alarm.period)

                      if past_load.nil?
                        hlog "Could not get past + #{@lookahead_window.inspect} total value for metric"
                        next
                      end

                      # now take the past total load and divide it by the
                      # current number of instances to get the predicted value
                      predicted_value = past_load.to_f / now_num
                      hlog "Predicted #{alarm.metric.name}: #{predicted_value}"

                      if check_alarm_threshold(alarm, predicted_value)
                        hlog "Executing policy"
                        policy.execute(honor_cooldown: true)

                        # don't need to evaluate further windows or policies on this group
                        return
                      end
                    end
                  end
                end
              end
            end
          end
        end
      end

      protected

      def check_alarm_threshold(alarm, value)
        case alarm.comparison_operator
        when "GreaterThanOrEqualToThreshold"
          value >= alarm.threshold
        when "GreaterThanThreshold"
          value > alarm.threshold
        when "LessThanThreshold"
          value < alarm.threshold
        when "LessThanOrEqualToThreshold"
          value <= alarm.threshold
        end
      end

      def load_for(group, metric, time, window)
        num_instances_metric = @cloudwatch.metrics.
          with_namespace("AWS/AutoScaling").
          with_metric_name("GroupInServiceInstances").
          filter('dimensions', [{
            :name => 'AutoScalingGroupName',
            :value => group.name
          }]).first

        unless num_instances_metric
          raise "Could not find GroupInServicesInstances metric for #{group.name}"
        end

        start_time = time - (window / 2)
        end_time = time + (window / 2)

        avg = average_for_metric(metric, start_time, end_time)
        num = average_for_metric(num_instances_metric, start_time, end_time)

        if avg.nil? || num.nil?
          return [ nil, nil ]
        end

        [ avg * num, num ]
      end

      def average_for_metric(metric, start_time, end_time)
        stats = metric.statistics(
          :start_time => start_time,
          :end_time => end_time,
          :statistics => [ "Average" ],
          :period => 60)

        return nil if stats.datapoints.length == 0

        sum = stats.datapoints.inject(0) do |r, dp|
          r + dp[:average]
        end

        sum.to_f / stats.datapoints.length
      end
    end
  end
end
