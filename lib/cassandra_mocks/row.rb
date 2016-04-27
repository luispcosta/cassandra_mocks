module Cassandra
  module Mocks
    class Row
      include MonitorMixin

      attr_reader :clusters, :partition_key

      def initialize(partition_key)
        @clusters = {}
        @partition_key = partition_key
        super()
      end

      def insert_record(clustering_columns, record_values, check_exists)
        synchronize do
          record_cluster = record_cluster(clustering_columns[0..-2])
          update_record(check_exists, clustering_columns, record_cluster, record_values)
        end
      end

      def find_records(clustering_columns)
        if @clusters.any?
          @clusters.map do |clustering_key, record|
            cluster_values(record, [clustering_key])
          end
        else
          []
        end
      end

      private

      def cluster_values(cluster, values)
        if cluster.is_a?(Record)
          [*partition_key, *values, *cluster.values]
        else
          cluster_values(cluster.values.first, values + [cluster.keys.first])
        end
      end

      def record_cluster(partial_clustering_columns)
        partial_clustering_columns.inject(clusters) do |memo, cluster_key|
          memo[cluster_key] ||= {}
        end
      end

      def update_record(check_exists, clustering_columns, record_cluster, record_values)
        previous_record = record_cluster[clustering_columns.last]
        if previous_record
          update_if_not_exists(check_exists, previous_record, record_values)
        else
          create_record(clustering_columns, record_cluster, record_values)
        end
      end

      def create_record(clustering_columns, record_cluster, record_values)
        record_cluster[clustering_columns.last] = Record.new(record_values)
        true
      end

      def update_if_not_exists(check_exists, previous_record, record_values)
        if check_exists
          false
        else
          previous_record.values = record_values
          true
        end
      end

    end
  end
end