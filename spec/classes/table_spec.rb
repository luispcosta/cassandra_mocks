require 'rspec'

module Cassandra
  module Mocks
    describe Keyspace do

      let(:keyspace_name) { 'keyspace' }
      let(:name) { 'table' }
      let(:pk_part_one) { Cassandra::Column.new('pk1', 'int', :asc) }
      let(:ck_part_one) { Cassandra::Column.new('ck1', 'string', :desc) }
      let(:fields_part_one) { Cassandra::Column.new('field1', 'double', :asc) }
      let(:pk_part_two) { Cassandra::Column.new('pk1', 'double', :asc) }
      let(:ck_part_two) { Cassandra::Column.new('ck1', 'int', :desc) }
      let(:fields_part_two) { Cassandra::Column.new('field1', 'string', :asc) }
      let(:partition_key) { [pk_part_one] }
      let(:clustering_key) { [ck_part_one] }
      let(:fields) { [fields_part_one] }
      let(:column_map) do
        (partition_key + clustering_key + fields).inject({}) do |memo, column|
          memo.merge!(column.name => column)
        end
      end
      let(:compaction) { Cassandra::Table::Compaction.new('mock', {}) }
      let(:options) { Cassandra::Table::Options.new({}, compaction, {}, false, 'mock') }
      let(:super_table) { Cassandra::Table.new(keyspace_name, name, partition_key, clustering_key, column_map, options, []) }

      subject { Table.new(keyspace_name, name, partition_key, clustering_key, fields) }

      it { is_expected.to eq(super_table) }

      context 'with a different table configuration' do
        let(:name) { 'broken table' }
        let(:partition_key) { [pk_part_one, pk_part_two] }
        let(:clustering_key) { [ck_part_one, ck_part_two] }
        let(:fields) { [fields_part_one, fields_part_two] }

        it { is_expected.to eq(super_table) }
      end

      describe '#insert' do
        let(:attributes) { {'pk1' => 15, 'ck1' => 'hello world'} }

        it 'should create a record from the input row' do
          subject.insert(attributes)
          expect(subject.rows).to eq([attributes])
        end

        context 'with multiple records' do
          let(:other_attributes) { {'pk1' => 45, 'ck1' => 'goodbye', 'field1' => 'world'} }

          it 'should be able to store multiple records' do
            subject.insert(attributes)
            subject.insert(other_attributes)
            expect(subject.rows).to match_array([attributes, other_attributes])
          end
        end

        context 'with a record containing invalid columns' do
          let(:attributes) { {'pk1' => 15, 'ck1' => 'hello world', 'field2' => 'stuff'} }

          it 'should raise an error' do
            expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, 'Invalid column, field2, specified')
          end

          context 'with an additional invalid column' do
            let(:attributes) { {'field3' => 'garbage', 'pk1' => 15, 'ck1' => 'hello world', 'field2' => 'stuff'} }

            it 'should raise the error on the first invalid column found' do
              expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, 'Invalid column, field3, specified')
            end
          end
        end

        context 'when missing a part of the primary key' do
          let(:attributes) { {'ck1' => 'hello world'} }

          it 'should raise the error on the first invalid column found' do
            expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, 'Invalid null primary key part, pk1')
          end

          context 'with different missing attributes' do
            let(:attributes) { {'pk1' => 53, 'ck1' => nil} }

            it 'should raise the error on the first invalid column found' do
              expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, 'Invalid null primary key part, ck1')
            end
          end
        end

      end

    end
  end
end