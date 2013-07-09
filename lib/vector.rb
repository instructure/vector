require 'aws-sdk'
require 'active_support/time'

require 'vector/version'

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

  module HLogger
    def hlog_ctx(ctx, &block)
      @components ||= []
      @components << ctx
      yield
    ensure
      @components.pop
    end

    def hlog(string)
      tmp_components = @components.dup
      level = 0
      if @last_components
        @last_components.each do |last_c|
          break if tmp_components.empty?
          if last_c == tmp_components[0]
            level += 1
            tmp_components.shift
          else
            break
          end
        end
      end

      tmp_components.each do |component|
        name = if component.respond_to? :name
                 component.name
               else
                 component.to_s
               end
        puts "#{"  " * level}#{name}"
        level += 1
      end

      puts "#{"  " * level}#{string}"
      @last_components = @components.dup
    end
  end
end

require 'vector/cli'
require 'vector/functions/predictive_scaling'
require 'vector/functions/flexible_down_scaling'
