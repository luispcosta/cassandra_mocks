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
            row[column].each_with_index.all? do |row_value, index|
              @comparitor.include?(row_value <=> value[index])
            end
          else
            @comparitor.include?(row[column] <=> value)
          end
        end

      end
    end
  end
end
