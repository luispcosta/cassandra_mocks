module Cassandra
  module Mocks
    class Statement
      class Token < Struct.new(:type, :value)
        def normalized_value
          case type
            when :int
              value.to_i
            when :float
              value.to_f
            else
              value
          end
        end

        def respond_to?(method)
          method_inquiry?(method) || super
        end

        def method_missing(method, *args)
          if method_inquiry?(method)
            method[/[^\?]+/].to_sym == type
          else
            super
          end
        end

        private

        def method_inquiry?(method)
          method =~ /\?$/
        end

      end
    end
  end
end
