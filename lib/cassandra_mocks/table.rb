module Cassandra
  module Mocks
    class Table < Cassandra::Table
      def initialize(keyspace, name, partition_key, clustering_key, fields)
        compaction = Cassandra::Table::Compaction.new('mock', {})
        options = Cassandra::Table::Options.new({}, compaction, {}, false, 'mock')
        column_map = column_map(partition_key, clustering_key, fields)
        super(keyspace, name, partition_key, clustering_key, column_map, options, [])
      end

      def insert(attributes, options = {})
        validate_columns!(attributes)
        validate_primary_key_presence!(attributes)

        prev_row_index = rows.find_index do |row|
          row.slice(*primary_key_names) == attributes.slice(*primary_key_names)
        end

        if prev_row_index
          return false if options[:check_exists]
          rows[prev_row_index] = attributes
        else
          rows << attributes
        end
        true
      end

      def select(*columns)
        filter = columns.pop if columns.last.is_a?(Hash)
        limit = (filter.delete(:limit) if filter)
        if filter && !filter.empty?
          validate_partion_key_filter!(filter)
          validate_clustering_column_filter!(filter)
        end

        filtered_rows = filtered_rows(filter)
        sorted_rows = filtered_rows.sort do |lhs, rhs|
          compare_rows(0, lhs, rhs)
        end

        sorted_rows = sorted_rows[0...limit] if limit

        result_rows = sorted_rows.map do |row|
          (columns.first == '*') ? row : row.slice(*columns)
        end
        ResultPage.new(result_rows)
      end

      def delete(filter)
        rows_to_remove = select('*', filter)
        @rows.reject! { |row| rows_to_remove.include?(row) }
      end

      def rows
        @rows ||= []
      end

      # make #partition_key public
      def partition_key
        super
      end

      # make #clustering_columns public
      def clustering_columns
        super
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

      def filtered_rows(filter)
        filter ? apply_filter(filter) : rows
      end

      def apply_filter(filter)
        rows.select { |row| row.slice(*filter.keys) == filter }
      end

      def validate_partion_key_filter!(filter)
        missing_partition_keys = Set.new(partition_key_names) - filter.keys
        raise Cassandra::Errors::InvalidError.new("Missing partition key part(s) #{missing_partition_keys.map(&:inspect) * ', '}", 'MockStatement') unless missing_partition_keys.empty?
      end

      def compare_rows(primary_key_index, lhs, rhs)
        return 0 if primary_key_names[primary_key_index].nil?

        if primary_key_part(lhs, primary_key_index) == primary_key_part(rhs, primary_key_index)
          compare_rows(primary_key_index + 1, lhs, rhs)
        else
          primary_key_part(lhs, primary_key_index) <=> primary_key_part(rhs, primary_key_index)
        end
      end

      def primary_key_part(row, primary_key_index)
        row[primary_key_names[primary_key_index]]
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
