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
          record_cluster = clustering_columns[0..-2].inject(clusters) do |memo, cluster_key|
            memo[cluster_key] ||= {}
          end

          previous_record = record_cluster[clustering_columns.last]
          if previous_record
            if check_exists
              false
            else
              previous_record.values = record_values
              true
            end
          else
            record_cluster[clustering_columns.last] = Record.new(record_values)
            true
          end
        end
      end

    end
  end
end