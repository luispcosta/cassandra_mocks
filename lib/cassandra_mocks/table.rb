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
        validate_columns!(attributes)
        validate_primary_key_presence!(attributes)

        rows << attributes
      end

      def select(*columns)
        filter = columns.pop if columns.last.is_a?(Hash)
        if filter
          validate_partion_key_filter!(filter)
          validate_clustering_column_filter!(filter)
        end

        selected_rows = if filter
                          rows.select { |row| row.slice(*filter.keys) == filter }
                        else
                          rows
                        end

        selected_rows.map do |row|
          (columns.first == '*') ? row : row.slice(*columns)
        end
      end

      def rows
        @rows ||= []
      end

      private

      def validate_columns!(attributes)
        attributes.keys.each do |column|
          unless column_names.include?(column)
            raise Errors::InvalidError.new(%Q{Invalid column, "#{column}", specified}, 'MockStatement')
          end
        end
      end

      def validate_primary_key_presence!(attributes)
        primary_key_names.each do |column|
          raise Errors::InvalidError.new(%Q{Invalid null primary key part, "#{column}"}, 'MockStatement') unless filter_has_column?(attributes, column)
        end
      end

      def validate_clustering_column_filter!(filter)
        prev_columns = []
        clustering_key_names.inject(true) do |hit_column, column|
          if filter_has_column?(filter, column)
            raise Cassandra::Errors::InvalidError.new("Clustering key part(s) #{prev_columns.map(&:inspect) * ', '} must be restricted", 'MockStatement') unless hit_column
          else
            prev_columns << column
          end
          filter_has_column?(filter, column)
        end
      end

      def filter_has_column?(filter, column)
        filter[column]
      end

      def validate_partion_key_filter!(filter)
        missing_partition_keys = Set.new(partition_key_names) - filter.keys
        raise Cassandra::Errors::InvalidError.new("Missing partition key part(s) #{missing_partition_keys.map(&:inspect) * ', '}", 'MockStatement') unless missing_partition_keys.empty?
      end

      def primary_key_names
        partition_key_names + clustering_key_names
      end

      def partition_key_names
        partition_key.map(&:name)
      end

      def clustering_key_names
        clustering_columns.map(&:name)
      end

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
