require 'rspec'

module Cassandra
  module Mocks
    describe Keyspace do

      let(:keyspace_name) { 'keyspace' }
      let(:name) { 'table' }
      let(:pk_part_one) { Cassandra::Column.new('pk1', 'int', :asc) }
      let(:ck_part_one) { Cassandra::Column.new('ck1', 'string', :desc) }
      let(:fields_part_one) { Cassandra::Column.new('field1', 'double', :asc) }
      let(:pk_part_two) { Cassandra::Column.new('pk2', 'double', :asc) }
      let(:ck_part_two) { Cassandra::Column.new('ck2', 'int', :desc) }
      let(:fields_part_two) { Cassandra::Column.new('field2', 'string', :asc) }
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

      describe '#select' do
        let(:attributes) { {'pk1' => 'partition', 'ck1' => 'clustering', 'field1' => 'some extra data'} }
        before { subject.insert(attributes) }

        it 'should return a list of all records inserted into the table' do
          expect(subject.select('*')).to eq([attributes])
        end

        context 'with multiple rows' do
          let(:attributes) { {'pk1' => 'other partition', 'ck1' => 'clustering'} }
          let(:other_attributes) { {'pk1' => 'other partition', 'ck1' => 'other clustering'} }

          before { subject.insert(other_attributes) }

          it 'should return all of the inserted rows' do
            expect(subject.select('*')).to match_array([attributes, other_attributes])
          end
        end

        context 'when selecting a specific column' do
          it 'should return on the key value pair for that column' do
            expect(subject.select('pk1')).to eq(['pk1' => 'partition'])
          end

          context 'when selecting multiple columns' do
            it 'should return on the key value pair for that column' do
              expect(subject.select('ck1', 'field1')).to eq(['ck1' => 'clustering', 'field1' => 'some extra data'])
            end
          end
        end

        describe 'filtering' do
          let(:attributes) do
            {'pk1' => 'partition',
             'pk2' => 'additional partition',
             'ck1' => 'clustering',
             'ck2' => 'additional clustering'}
          end
          let(:partition_key) { [pk_part_one, pk_part_two] }
          let(:clustering_key) { [ck_part_one, ck_part_two] }

          before do
            (1..2).each do |pk1|
              (1..2).each do |pk2|
                (1..2).each do |ck1|
                  (1..2).each do |ck2|
                    subject.insert({
                                       'pk1' => "partition #{pk1}",
                                       'pk2' => "additional partition #{pk2}",
                                       'ck1' => "clustering #{ck1}",
                                       'ck2' => "additional clustering #{ck2}",
                                   })
                  end
                end
              end
            end
          end

          describe 'filtering by partition key' do
            it 'should return all records for that partition' do
              expected_results = [{'pk1' => 'partition', 'pk2' => 'additional partition', 'ck1' => 'clustering', 'ck2' => 'additional clustering'}]
              expect(subject.select('*', {'pk1' => 'partition', 'pk2' => 'additional partition'})).to eq(expected_results)
            end

            context 'with a different partition key' do
              it 'should return all records for that partition' do
                expected_results = [
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1'},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2'},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 1'},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2'},
                ]
                expect(subject.select('*', {'pk1' => 'partition 2', 'pk2' => 'additional partition 2'})).to eq(expected_results)
              end
            end

            it 'should raise an error if only specifying part of the partition key' do
              expect { (subject.select('*', {'pk1' => 'partition'})) }.to raise_error(Cassandra::Errors::InvalidError, 'Missing partition key part(s) pk2')
            end

            context 'with a different part missing' do
              it 'should raise an error' do
                expect { (subject.select('*', {'pk2' => 'additional partition'})) }.to raise_error(Cassandra::Errors::InvalidError, 'Missing partition key part(s) pk1')
              end
            end

            context 'with both parts missing' do
              it 'should raise an error' do
                expect { (subject.select('*', {'ck1' => 'clustering'})) }.to raise_error(Cassandra::Errors::InvalidError, 'Missing partition key part(s) pk1, pk2')
              end
            end
          end

          describe 'filtering by clustering columns' do
            it 'should return all records for that partition, matching the specified clustering columns' do
              expected_results = [
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1'},
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2'},
              ]
              expect(subject.select('*', {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1'})).to eq(expected_results)
            end

            context 'with multiple clustering columns specified' do
              it 'should return all records for that partition, matching the specified clustering columns' do
                expected_results = [
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1'},
                ]
                expect(subject.select('*', {'pk1' => 'partition 2',
                                            'pk2' => 'additional partition 2',
                                            'ck1' => 'clustering 1',
                                            'ck2' => 'additional clustering 1'})).to eq(expected_results)
              end
            end
          end

        end

      end

    end
  end
end
