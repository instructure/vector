require 'aws-sdk'
require 'active_support/time'

require 'vector/version'
require 'vector/cli'
require 'vector/functions/predictive_scaling'
require 'vector/functions/flexible_down_scaling'

module Vector
  def self.time_string_to_seconds(string)
    if string =~ /^(\d+)([smhdw])?$/
      n = $1.to_i
      unit = $2 || 's'

      case unit
      when 's'
        n.seconds
      when 'm'
        n.minutes
      when 'h'
        n.hours
      when 'd'
        n.days
      when 'w'
        n.weeks
      end
    else
      nil
    end
  end

  def self.within_threshold(threshold, v1, v2)
    threshold * v1 < v2 && threshold * v2 < v1
  end
end
