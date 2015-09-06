require 'rspec'

module Cassandra
  module Mocks
    describe Keyspace do

      let(:keyspace_name) { 'keyspace' }
      let(:keyspace) { Keyspace.new(keyspace_name) }
      let(:name) { 'table' }
      let(:pk_part_one) { Cassandra::Column.new('pk1', 'int', :asc) }
      let(:ck_part_one) { Cassandra::Column.new('ck1', 'string', :desc) }
      let(:fields_part_one) { Cassandra::Column.new('field1', 'double', :asc) }
      let(:partition_key) { [pk_part_one] }
      let(:clustering_key) { [ck_part_one] }
      let(:fields) { [fields_part_one] }
      let(:compaction) { Cassandra::Table::Compaction.new('mock', {}) }
      let(:options) { Cassandra::Table::Options.new({}, compaction, {}, false, 'mock') }
      let(:super_table) { Cassandra::Table.new(keyspace, name, partition_key, clustering_key, fields, options, []) }

      subject { Table.new(keyspace, name, partition_key, clustering_key, fields) }

      it { is_expected.to eq(super_table) }

      context 'with a different table configuration' do
        let(:name) { 'broken table' }
        let(:pk_part_two) { Cassandra::Column.new('pk1', 'double', :asc) }
        let(:ck_part_two) { Cassandra::Column.new('ck1', 'int', :desc) }
        let(:fields_part_two) { Cassandra::Column.new('field1', 'string', :asc) }
        let(:partition_key) { [pk_part_one, pk_part_two] }
        let(:clustering_key) { [ck_part_one, ck_part_two] }
        let(:fields) { [fields_part_one, fields_part_two] }

        it { is_expected.to eq(super_table) }
      end

    end
  end
end