module Cassandra
  module Mocks
    class Table < Cassandra::Table
      extend Forwardable

      def_delegator :@rows, :clear

      def initialize(keyspace, name, partition_key, clustering_key, fields)
        @rows = Concurrent::Map.new { |hash, key| hash[key] = Row.new(key) }

        compaction = Cassandra::Table::Compaction.new('mock', {})
        options = Cassandra::Table::Options.new({}, compaction, {}, false, 'mock')
        column_map = column_map(partition_key, clustering_key, fields)

        @counter_column = fields.find { |field| field.type.to_sym == :counter }
        has_non_counters = !!fields.find { |field| field.type.to_sym != :counter }

        if counter_column && has_non_counters
          raise Cassandra::Errors::ConfigurationError.new("Cannot add counter column '#{counter_column.name}' to non-counter column family", 'MockStatement')
        end

        super(keyspace, name, partition_key, clustering_key, column_map, options, [])
        @column_map = columns.inject({}) do |memo, column|
          memo.merge!(column.name => column)
        end
        @partition_key_set = Set.new(partition_key_names)
      end

      def rows
        results = []
        @rows.each_pair do |_, row|
          row_results = record_attributes(row.find_records([]))
          results << row_results
        end
        results.flatten
      end

      def counter_table?
        !!counter_column
      end

      def insert(attributes, options = {})
        validate_columns!(attributes)
        validate_primary_key_presence!(attributes)

        row = @rows[attribute_partition_key(attributes)]
        row.insert_record(attribute_clustering_key(attributes), attribute_fields(attributes), options[:check_exists])
      end

      def select(*columns)
        filter = select_filter(columns)
        limit = filter.delete(:limit)
        order = select_order(filter)

        order_keys_in_partition = order.keys.select { |column| partition_key_names.include?(column) }
        if order_keys_in_partition.any?
          raise Cassandra::Errors::InvalidError.new("Order by is currently only supported on the clustered columns of the PRIMARY KEY, got #{order_keys_in_partition * ', '}", 'MockStatement')
        end

        missing_ordering_keys = order.keys.select { |column| !column_names.include?(column) }
        if missing_ordering_keys.any?
          raise Cassandra::Errors::InvalidError.new("Order by on unknown column(s) #{missing_ordering_keys * ', '}", 'MockStatement')
        end

        out_of_order = order.keys.each.with_index.any? { |column, index| clustering_key_names[index] != column }
        if out_of_order
          raise Cassandra::Errors::InvalidError.new("Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY (expected #{clustering_key_names * ', '} got #{order.keys * ', '})", 'MockStatement')
        end

        inconsistent_order = (order.values.uniq.count > 1)
        if inconsistent_order
          raise Cassandra::Errors::InvalidError.new('Ordering direction must be consistent across all clustering columns', 'MockStatement')
        end

        filter = filter.fetch(:restriction) { {} }
        validate_filter!(filter)

        filtered_rows = if filter.any?
                          partition_key = attribute_partition_key(filter)
                          partition_range = (partition_key.pop if partition_key.last.is_a?(Array))
                          cluster_filter = filter.except(*partition_key_names)
                          cluster_key = attribute_partial_clustering_key(cluster_filter)
                          _, cluster_slice = cluster_filter.find do |_, value|
                            restriction_is_slice?(value)
                          end

                          records = partially_filtered_rows(partition_range, partition_key, cluster_key)
                          rows = record_attributes(records)
                          filtered_rows(cluster_slice, rows)
                        else
                          self.rows
                        end

        sorted_rows = filtered_rows.sort do |lhs, rhs|
          compare_rows(0, lhs, rhs, order)
        end

        sorted_rows = sorted_rows[0...limit] if limit

        result_rows = sorted_rows.map do |row|
          (columns.first == '*') ? row : row.slice(*columns)
        end
        ResultPage.new(result_rows)
      end

      def delete(filter)
        validate_filter!(filter)
        row = @rows[attribute_partition_key(filter)]
        row.delete_records(attribute_partial_clustering_key(filter))
      end

      def attribute_partition_key(attributes)
        partition_key_names.map { |column| attributes[column] }
      end

      def attribute_clustering_key(attributes)
        clustering_key_names.map do |column|
          value = attributes[column]
          value unless restriction_is_slice?(value)
        end
      end

      def attribute_partial_clustering_key(attributes)
        attribute_clustering_key(attributes).compact
      end

      def attribute_fields(attributes)
        field_names.map { |column| attributes[column] }
      end

      def restriction_is_slice?(value)
        value.is_a?(Statement::Comparitor) || value.is_a?(Array)
      end

      def validate_filter!(filter)
        unless filter.empty?
          validate_range_query!(filter)
          validate_partition_key_filter!(filter)
          validate_clustering_column_filter!(filter)
          raise_if_fields_restricted!(filter)
        end
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

      attr_reader :counter_column
      def_delegator :@column_map, :[], :find_column

      def select_filter(columns)
        columns.last.is_a?(Hash) ? columns.pop.dup : {}
      end

      def select_order(filter)
        (filter.delete(:order) || {}).inject(Hash.new(1)) do |memo, (key, value)|
          memo.merge!(key => value == :asc ? 1 : -1)
        end
      end

      def validate_columns!(attributes)
        attributes.each do |column_name, value|
          column = find_column(column_name)
          unless column
            raise Errors::InvalidError.new(%Q{Invalid column, "#{column_name}", specified}, 'MockStatement')
          end

          if value
            case column.type.to_sym
              when :double
                raise_unless_valid_type(column_name, Float, value)
              when :string
                raise_unless_valid_type(column_name, String, value)
              when :text
                raise_unless_valid_type(column_name, String, value)
              when :varchar
                raise_unless_valid_type(column_name, String, value)
              when :blob
                raise_unless_valid_type(column_name, String, value)
              when :int
                raise_unless_valid_type(column_name, Fixnum, value)
              when :uuid
                raise_unless_valid_type(column_name, Cassandra::Uuid, value)
              when :timeuuid
                raise_unless_valid_type(column_name, Cassandra::TimeUuid, value)
              when :timestamp
                raise_unless_valid_type(column_name, Time, value)
            end
          end
        end
      end

      def raise_unless_valid_type(column_name, ruby_type, value)
        unless value.is_a?(ruby_type)
          raise Errors::InvalidError.new(%Q{Expected column "#{column_name}" to be of type "#{ruby_type}", got a(n) "#{value.class}"}, 'MockStatement')
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

      def raise_if_fields_restricted!(filter)
        fields = column_names - (partition_key_names + clustering_key_names)
        raise Cassandra::Errors::InvalidError.new('Filtering by fields is not supported', 'MockStatement') if fields.any? { |field| filter.keys.include?(field) }
      end

      def filter_has_column?(filter, column)
        filter[column]
      end

      def validate_range_query!(filter)
        invalid_partition_range_column = partition_key_names[0..-2].find { |column| filter[column].is_a?(Array) }
        if invalid_partition_range_column
          raise Cassandra::Errors::InvalidError.new("Partition KEY part #{invalid_partition_range_column} cannot be restricted by IN relation (only the last part of the partition key can)", 'MockStatement')
        end
      end

      def validate_partition_key_filter!(filter)
        missing_partition_keys = @partition_key_set - filter.keys
        raise Cassandra::Errors::InvalidError.new("Missing partition key part(s) #{missing_partition_keys.map(&:inspect) * ', '}", 'MockStatement') unless missing_partition_keys.empty?
      end

      def compare_rows(primary_key_index, lhs, rhs, order)
        return 0 if primary_key_names[primary_key_index].nil?

        if primary_key_part(lhs, primary_key_index) == primary_key_part(rhs, primary_key_index)
          compare_rows(primary_key_index + 1, lhs, rhs, order)
        else
          comparison = primary_key_part(lhs, primary_key_index) <=> primary_key_part(rhs, primary_key_index)
          order_comparison(comparison, order, primary_key_names[primary_key_index])
        end
      end

      def order_comparison(comparison, order, primary_key)
        comparison * order[primary_key]
      end

      def primary_key_part(row, primary_key_index)
        row[primary_key_names[primary_key_index]]
      end

      def record_attributes(records)
        records.map do |record|
          attributes = {}
          column_names.each.with_index do |column, index|
            attributes[column] = record[index]
          end
          attributes
        end
      end

      def partially_filtered_rows(partition_range, partition_key, cluster_key)
        if partition_range
          partition_range_records(partition_range, partition_key, cluster_key)
        else
          records_for_row(partition_key, cluster_key)
        end
      end

      def partition_range_records(partition_range, partition_key, cluster_key)
        partition_range.map do |part|
          records_for_row(partition_key + [part], cluster_key)
        end.flatten(1)
      end

      def records_for_row(partition_key, cluster_key)
        row = @rows[partition_key]
        row.find_records(cluster_key)
      end

      def filtered_rows(cluster_slice, rows)
        cluster_slice ? apply_filter(cluster_slice, rows) : rows
      end

      def apply_filter(cluster_slice, rows)
        rows.select do |row|
          partial_row = partial_row(cluster_slice, row)

          if cluster_slice.is_a?(Statement::Comparitor)
            cluster_slice.check_against(partial_row)
          elsif cluster_slice.is_a?(Array)
            cluster_slice.all? { |value| value.check_against(partial_row) }
          end
        end
      end

      def partial_row(cluster_slice, row)
        comparitor = cluster_slice.is_a?(Statement::Comparitor) ? cluster_slice : cluster_slice.first
        value = if comparitor.column.is_a?(Array)
                  row.values_at(*comparitor.column)
                else
                  row[comparitor.column]
                end
        {comparitor.column => value}
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

      def field_names
        column_names - (partition_key_names + clustering_key_names)
      end

      def column_map(partition_key, clustering_key, fields)
        (partition_key + clustering_key + fields).inject({}) do |memo, column|
          memo.merge!(column.name => column)
        end
      end

    end
  end
end
