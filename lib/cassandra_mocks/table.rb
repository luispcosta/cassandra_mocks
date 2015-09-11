module Cassandra
  module Mocks
    class Table < Cassandra::Table
      def initialize(keyspace, name, partition_key, clustering_key, fields)
        compaction = Cassandra::Table::Compaction.new('mock', {})
        options = Cassandra::Table::Options.new({}, compaction, {}, false, 'mock')
        column_map = column_map(partition_key, clustering_key, fields)
        super(keyspace, name, partition_key, clustering_key, column_map, options, [])
      end

      def insert(attributes)
        attributes.keys.each do |column|
          unless column_names.include?(column)
            raise Errors::InvalidError.new("Invalid column, #{column}, specified", 'MockStatement')
          end
        end

        rows << attributes
      end

      def rows
        @rows ||= []
      end

      private

      def column_names
        columns.map(&:name)
      end

      def column_map(partition_key, clustering_key, fields)
        (partition_key + clustering_key + fields).inject({}) do |memo, column|
          memo.merge!(column.name => column)
        end
      end

    end
  end
end