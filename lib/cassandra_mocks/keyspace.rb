module Cassandra
  module Mocks
    class Keyspace < ::Cassandra::Keyspace

      def initialize(name)
        replication = Replication.new('mock', {})
        super(name, false, replication, {})
      end

      def add_table(table_name, primary_key, columns)
        partition_key = primary_key.shift
        partition_key_columns = partition_key_part(columns, partition_key)
        clustering_columns = partition_key_part(columns, primary_key)
        fields = fields(columns, partition_key, primary_key)
        @tables[table_name] = Table.new(name, table_name, partition_key_columns, clustering_columns, fields)
      end

      private

      def partition_key_part(columns, primary_key)
        primary_key.map { |name| Cassandra::Column.new(name, columns[name], :asc) }
      end

      def fields(columns, partition_key, primary_key)
        columns.except(partition_key + primary_key).map { |name, type| Cassandra::Column.new(name, type, :asc) }
      end

    end
  end
end
