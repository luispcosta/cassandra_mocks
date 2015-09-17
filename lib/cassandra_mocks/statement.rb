module Cassandra
  module Mocks
    class Statement
      attr_reader :cql, :action, :args

      def initialize(cql, args)
        @cql = cql
        @action = :insert

        if cql =~ /INSERT/i
          insert_keys = cql.match(/\(\s*([^(]+)\s*\)\s*VALUES/i)[1].split(',').map(&:strip)
          insert_args = cql.match(/VALUES\s*\(\s*([^(]+)\s*\)/i)[1].split(',').map(&:strip)
          @args = insert_keys.count.times.inject({}) do |memo, index|
            memo.merge!(insert_keys[index] => insert_args[index][1..-2])
          end
        end
      end


    end
  end
end
