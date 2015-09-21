module Cassandra
  module Mocks
    class Keyspace < ::Cassandra::Keyspace

      def initialize(name)
        replication = Cassandra::Keyspace::Replication.new('mock', {})
        super(name, false, replication, {})
      end

      def add_table(name, primary_key, columns)
        partition_key = primary_key.shift
        partition_key_columns = partition_key.map { |name| Cassandra::Column.new(name, columns[name], :asc) }
        clustering_columns = primary_key.map { |name| Cassandra::Column.new(name, columns[name], :asc) }
        fields = columns.except(partition_key + primary_key).map { |name, type| Cassandra::Column.new(name, type, :asc) }
        @tables[name] = Cassandra::Mocks::Table.new(self.name,
                                             name,
                                             partition_key_columns,
                                             clustering_columns,
                                             fields)
      end

    end
  end
end
