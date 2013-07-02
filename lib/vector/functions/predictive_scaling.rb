module Vector
  module Function
    class PredictiveScaling
      def initialize(options)
        @cloudwatch = AWS::CloudWatch.new

        @lookback_windows = options['lookback-windows'] || []
        @lookback_windows = @lookback_windows.map do |w|
          [ w, Vector.time_string_to_seconds(w) ]
        end

        @lookahead_window = Vector.time_string_to_seconds(
          options['lookahead-window'])

        @valid_threshold = options['valid-threshold'].to_f

        @valid_period = Vector.time_string_to_seconds options['valid-period']
      end

      def run_for(group)
        return if @lookback_windows.length == 0

        group.scaling_policies.each do |policy|
          # only examine scaleup policies
          next if policy.scaling_adjustment <= 0

          policy.alarms.keys.each do |alarm_name|
            alarm = @cloudwatch.alarms[alarm_name]
            next unless alarm.enabled?

            @lookback_windows.each do |window_name, window_time|
              now_load, now_num = load_for(group, alarm,
                Time.now, @valid_period)

              then_load, = load_for(group, alarm,
                Time.now - window_time, @valid_period)

              # check that the past total utilization is within
              # threshold% of the current total utilization
              if !Vector.within_threshold(@load_threshold,
                                          now_load, then_load)
                next
              end

              past_load, = load_for(group,
                Time.now - window_time + @lookahead_window,
                alarm.period)

              # now take the past total load and divide it by the
              # current number of instances to get the predicted avg
              # load
              predicted_load = past_load.to_f / now_num

              if check_alarm_threshold(alarm, predicted_load)
                policy.execute
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
          }])

        start_time = time - (window / 2)
        end_time = time + (window / 2)

        avg = average_for_metric(metric, start_time, end_time)
        num = average_for_metric(num_instances_metric, start_time, end_time)

        if avg.nil? || num.nil?
          raise "Could not get load for #{group.name} #{metric.name}"
        end

        [ avg * num, num ]
      end

      def average_for_metric(metric, start_time, stop_time)
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
