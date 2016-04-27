module Cassandra
  module Mocks
    class Row
      include MonitorMixin

      attr_reader :clusters

      def initialize
        @clusters = {}
        super
      end

      def insert_record(clustering_columns, record_values, check_exists)
        synchronize do
          record_cluster = record_cluster(clustering_columns)
          update_record(check_exists, clustering_columns, record_cluster, record_values)
        end
      end

      private

      def record_cluster(clustering_columns)
        clustering_columns[0..-2].inject(clusters) do |memo, cluster_key|
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