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
      end
    end
  end
end
