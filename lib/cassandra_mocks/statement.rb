module Cassandra
  module Mocks
    class Statement
      attr_reader :cql, :action, :args

      PARENS_MATCHER = /\(\s*([^(]+)\s*\)/

      def initialize(cql, args)
        @cql = cql
        @action = :insert

        parse_insert_query(cql, args) if cql =~ /INSERT/i
      end

      private

      def parse_insert_query(cql, args)
        insert_keys, insert_values = parsed_insert_args(cql)
        @args = insert_args(insert_keys, insert_values, args)
      end

      def insert_args(insert_keys, insert_values, args)
        param_index = -1
        insert_keys.count.times.inject({}) do |memo, index|
          value = insert_values[index]
          value = (value == '?') ? args[param_index+=1] : value[1..-2]
          memo.merge!(insert_keys[index] => value)
        end
      end

      def parsed_insert_args(cql)
        matched_insert_args(cql).map { |match| match.split(',').map(&:strip) }
      end

      def matched_insert_args(cql)
        cql.match(/#{PARENS_MATCHER}\s*VALUES\s*#{PARENS_MATCHER}/i)[1..2]
      end

    end
  end
end
