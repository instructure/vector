module Vector
  module Function
    class PredictiveScaling
      def initialize(options)
        @cloudwatch = options[:cloudwatch]
        @lookback_windows = options[:lookback_windows]
        @lookahead_window = options[:lookahead_window]
        @valid_threshold = options[:valid_threshold]
        @valid_period = options[:valid_period]
      end

      def run_for(group)
        return if @lookback_windows.length == 0

        group.scaling_policies.each do |policy|
          # only examine scaleup policies
          next if policy.scaling_adjustment <= 0

          policy.alarms.keys.each do |alarm_name|
            alarm = @cloudwatch.alarms[alarm_name]
            next unless alarm.enabled?

            now_load, now_num = load_for(group, alarm.metric,
              Time.now, @valid_period)

            if now_load.nil?
              puts "#{group.name}: could not get current load for metric #{alarm.metric.name}"
              next
            end

            @lookback_windows.each do |window|
              then_load, = load_for(group, alarm.metric,
                Time.now - window, @valid_period)

              if then_load.nil?
                puts "#{group.name}: could not get -#{window.inspect} load for metric #{alarm.metric.name}"
                next
              end

              # check that the past total utilization is within
              # threshold% of the current total utilization
              if !Vector.within_threshold(@valid_threshold,
                                          now_load, then_load)
                puts "#{group.name}: past load not within threshold (current #{now_load}, then #{then_load})"
                next
              end

              past_load, = load_for(group, alarm.metric,
                Time.now - window + @lookahead_window,
                alarm.period)

              if past_load.nil?
                puts "#{group.name}: could not get -#{window.inspect} +#{@lookahead_window.inspect} load for metric #{alarm.metric.name}"
                next
              end

              # now take the past total load and divide it by the
              # current number of instances to get the predicted avg
              # load
              predicted_load = past_load.to_f / now_num
              puts "#{group.name}: predicted load: #{predicted_load}"

              if check_alarm_threshold(alarm, predicted_load)
                puts "#{group.name}: executing policy #{policy.name}"
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
