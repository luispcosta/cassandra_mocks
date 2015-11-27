module Cassandra
  module Mocks
    class Statement
      class Comparitor < Struct.new(:operation, :column, :value)

        COMPARISON_MAP = {
            lt: [-1],
            le: [-1, 0],
            eq: [0],
            ge: [0, 1],
            gt: [1]
        }

        def initialize(*args)
          super
          @comparitor = COMPARISON_MAP[operation]
        end

        def check_against(row)
          if column.is_a?(Array)
            check_against_array(row[column])
          else
            @comparitor.include?(row[column] <=> value)
          end
        end

        def check_against_array(row_values, index = 0, prev_result = false)
          row_value = row_values[index]
          comparison_value = value[index]
          return prev_result unless row_value

          comparison = @comparitor.include?(row_value <=> comparison_value)
          return comparison if row_value != comparison_value

          check_against_array(row_values, index+1, comparison)
        end

      end
    end
  end
end
