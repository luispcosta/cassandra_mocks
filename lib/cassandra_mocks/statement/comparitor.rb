module Cassandra
  module Mocks
    class Statement
      class Comparitor < Struct.new(:operation, :column, :value)

        def check_against(row)
          column_value = row[column]
          case operation
            when :lt
              column_value < value
            when :le
              column_value <= value
            when :eq
              column_value == value
            when :ge
              column_value >= value
            when :gt
              column_value > value
          end
        end

      end
    end
  end
end
