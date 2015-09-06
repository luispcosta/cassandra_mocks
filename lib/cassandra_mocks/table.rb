module Cassandra
  module Mocks
    class Table < Cassandra::Table

      def initialize(keyspace, name, partition_key, clustering_key, fields)
        compaction = Cassandra::Table::Compaction.new('mock', {})
        options = Cassandra::Table::Options.new({}, compaction, {}, false, 'mock')
        super(keyspace, name, partition_key, clustering_key, fields, options, [])
      end

    end
  end
end