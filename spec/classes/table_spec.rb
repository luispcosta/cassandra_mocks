require 'rspec'

module Cassandra
  module Mocks
    describe Keyspace do

      let(:keyspace_name) { 'keyspace' }
      let(:name) { 'table' }
      let(:pk_part_one) { Cassandra::Column.new('pk1', 'string', :asc) }
      let(:ck_part_one) { Cassandra::Column.new('ck1', 'string', :desc) }
      let(:fields_part_one) { Cassandra::Column.new('field1', 'string', :asc) }
      let(:pk_part_two) { Cassandra::Column.new('pk2', 'string', :asc) }
      let(:ck_part_two) { Cassandra::Column.new('ck2', 'string', :desc) }
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

      context 'when mixing counter fields with non counter fields in the table' do
        let(:field_name_one) { Faker::Lorem.word }
        let(:fields_part_one) { Cassandra::Column.new(field_name_one, 'counter', :asc) }
        let(:field_name_two) { Faker::Lorem.word }
        let(:fields_part_two) { Cassandra::Column.new(field_name_two, 'int', :asc) }
        let(:fields) { [fields_part_one, fields_part_two] }

        it 'should not allow this' do
          expect { subject }.to raise_error(Cassandra::Errors::ConfigurationError, "Cannot add counter column '#{field_name_one}' to non-counter column family")
        end
      end

      describe '#counter_table?' do
        its(:counter_table?) { is_expected.to eq(false) }

        context 'when a counter column is present' do
          let(:fields_part_one) { Cassandra::Column.new('some_count', 'counter', :asc) }

          its(:counter_table?) { is_expected.to eq(true) }
        end
      end

      context 'with a different table configuration' do
        let(:name) { 'broken table' }
        let(:partition_key) { [pk_part_one, pk_part_two] }
        let(:clustering_key) { [ck_part_one, ck_part_two] }
        let(:fields) { [fields_part_one, fields_part_two] }

        it { is_expected.to eq(super_table) }
      end

      describe '#insert' do
        let(:attributes) { {'pk1' => '15', 'ck1' => 'hello world', 'field1' => nil} }

        it 'should create a record from the input row' do
          subject.insert(attributes)
          expect(subject.rows).to eq([attributes])
        end

        context 'with an invalid column type' do
          shared_examples_for 'a column type validation' do |cassandra_type, ruby_type, bad_type_sample, bad_type_sample_two|
            let(:fields_part_one) { Cassandra::Column.new('field1', cassandra_type, :asc) }
            let(:sample_bad_type) { bad_type_sample.class }
            let(:sample_bad_type_two) { bad_type_sample_two.class }
            let(:attributes) { {'pk1' => '15', 'ck1' => 'hello world', 'field1' => bad_type_sample} }

            it 'should raise an error' do
              expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, %Q{Expected column "field1" to be of type "#{ruby_type}", got a(n) "#{sample_bad_type}"})
            end

            context 'with a different value' do
              let(:attributes) { {'pk1' => '15', 'ck1' => 'hello world', 'field1' => bad_type_sample_two} }

              it 'should raise an error' do
                expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, %Q{Expected column "field1" to be of type "#{ruby_type}", got a(n) "#{sample_bad_type_two}"})
              end
            end

            context 'with a different column' do
              let(:attributes) { {'pk1' => '15', 'ck1' => 'hello world', 'field3' => bad_type_sample} }
              let(:fields_part_one) { Cassandra::Column.new('field3', cassandra_type, :asc) }

              it 'should raise an error' do
                expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, %Q{Expected column "field3" to be of type "#{ruby_type}", got a(n) "#{sample_bad_type}"})
              end
            end
          end

          it_behaves_like 'a column type validation', :double, Float, 'world', 54
          it_behaves_like 'a column type validation', 'double', Float, 'world', 54
          it_behaves_like 'a column type validation', :string, String, 5.0, 57
          it_behaves_like 'a column type validation', :text, String, 5.0, 57
          it_behaves_like 'a column type validation', :varchar, String, 5.0, 57
          it_behaves_like 'a column type validation', :int, Fixnum, 5.0, Cassandra::Uuid.new(SecureRandom.uuid)
          it_behaves_like 'a column type validation', :uuid, Cassandra::Uuid, SecureRandom.uuid, 55444
          it_behaves_like 'a column type validation', :timeuuid, Cassandra::TimeUuid, SecureRandom.uuid, 55444
          it_behaves_like 'a column type validation', :timestamp, Time, '10:30:55', 1000
          it_behaves_like 'a column type validation', :blob, String, 99, 5.0
        end

        context 'with multiple records' do
          let(:other_attributes) { {'pk1' => '45', 'ck1' => 'goodbye', 'field1' => 'world'} }

          it 'should be able to store multiple records' do
            subject.insert(attributes)
            subject.insert(other_attributes)
            expect(subject.rows).to match_array([attributes, other_attributes])
          end
        end

        context 'with records containing the same primary key' do
          let(:attributes) { {'pk1' => '10', 'ck1' => 'hello', 'field1' => 'world'} }
          let(:other_attributes) { {'pk1' => '10', 'ck1' => 'hello', 'field1' => 'planet'} }

          it 'should only keep the latest copy of the record' do
            subject.insert(attributes)
            subject.insert(other_attributes)
            expect(subject.rows).to eq([other_attributes])
          end

          it 'should return true' do
            expect(subject.insert(other_attributes, check_exists: true)).to eq(true)
          end

          context 'when requested to reject new entries' do
            before do
              subject.insert(attributes, check_exists: true)
            end

            it 'should only keep the oldest copy of the record' do
              subject.insert(other_attributes, check_exists: true)
              expect(subject.rows).to eq([attributes])
            end

            it 'should return false' do
              expect(subject.insert(other_attributes, check_exists: true)).to eq(false)
            end
          end

          context 'with a different set of rows' do
            let(:attributes) { {'pk1' => '10', 'ck1' => 'hello', 'field1' => 'world'} }
            let(:attributes_two) { {'pk1' => '10', 'ck1' => 'goodbye', 'field1' => 'planet'} }
            let(:attributes_three) { {'pk1' => '10', 'ck1' => 'goodbye', 'field1' => 'world'} }

            it 'should keep only unique records' do
              subject.insert(attributes)
              subject.insert(attributes_two)
              subject.insert(attributes_three)
              expect(subject.rows).to match_array([attributes, attributes_three])
            end
          end

          context 'with a different partition key, but the same clustering key' do
            let(:attributes) { {'pk1' => '10', 'ck1' => 'hello', 'field1' => 'world'} }
            let(:other_attributes) { {'pk1' => '11', 'ck1' => 'hello', 'field1' => 'planet'} }

            it 'should keep both records' do
              subject.insert(attributes)
              subject.insert(other_attributes)
              expect(subject.rows).to match_array([attributes, other_attributes])
            end
          end
        end

        context 'with a record containing invalid columns' do
          let(:attributes) { {'pk1' => '15', 'ck1' => 'hello world', 'field2' => 'stuff'} }

          it 'should raise an error' do
            expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, 'Invalid column, "field2", specified')
          end

          context 'with an additional invalid column' do
            let(:attributes) { {'field3' => 'garbage', 'pk1' => '15', 'ck1' => 'hello world', 'field2' => 'stuff'} }

            it 'should raise the error on the first invalid column found' do
              expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, 'Invalid column, "field3", specified')
            end
          end
        end

        context 'when missing a part of the primary key' do
          let(:attributes) { {'ck1' => 'hello world'} }

          it 'should raise the error on the first invalid column found' do
            expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, 'Invalid null primary key part, "pk1"')
          end

          context 'with different missing attributes' do
            let(:attributes) { {'pk1' => '53', 'ck1' => nil} }

            it 'should raise the error on the first invalid column found' do
              expect { subject.insert(attributes) }.to raise_error(Cassandra::Errors::InvalidError, 'Invalid null primary key part, "ck1"')
            end
          end
        end

      end

      describe '#clear' do
        let(:attributes) { {'pk1' => '15', 'ck1' => 'hello world', 'field1' => nil} }

        before { subject.insert(attributes) }

        it 'should remove all records' do
          subject.clear
          expect(subject.rows).to be_empty
        end

      end

      describe '#select' do
        let(:attributes) { {'pk1' => 'partition', 'ck1' => 'clustering', 'field1' => 'some extra data'} }
        before { subject.insert(attributes) }

        it 'should return a list of all records inserted into the table' do
          expect(subject.select('*')).to eq([attributes])
        end

        it 'should result a ResultPage' do
          expect(subject.select('*')).to be_a_kind_of(ResultPage)
        end

        context 'with multiple rows' do
          let(:attributes) { {'pk1' => 'other partition', 'ck1' => 'clustering', 'field1' => nil} }
          let(:attributes_two) { {'pk1' => 'other partition', 'ck1' => 'other clustering', 'field1' => nil} }
          let(:attributes_three) { {'pk1' => 'other partition', 'ck1' => 'other clustering2', 'field1' => nil} }

          before do
            subject.insert(attributes_two)
            subject.insert(attributes_three)
          end

          it 'should return all of the inserted rows' do
            expect(subject.select('*')).to match_array([attributes, attributes_two, attributes_three])
          end

          context 'with a limit specified' do
            it 'should limit the returned results to the specified number of rows' do
              expect(subject.select('*', limit: 1)).to match_array([attributes])
            end

            context 'with a different limit' do
              it 'should limit the returned results to the specified number of rows' do
                expect(subject.select('*', limit: 2)).to match_array([attributes, attributes_two])
              end
            end
          end
        end

        describe 'sorting' do
          let(:attributes) do
            {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 3'}
          end
          let(:partition_key) { [pk_part_one, pk_part_two] }
          let(:clustering_key) { [ck_part_one, ck_part_two] }

          it 'should sort by partition key, then by clustering columns' do
            subject.insert({'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2'})
            expected_results = [
                {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2', 'field1' => nil},
                {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 3', 'field1' => nil},
            ]
            expect(subject.select('*')).to eq(expected_results)
          end

          context 'when the first partition part is in order, by the second is not' do
            it 'should sort by partition key, then by clustering columns' do
              subject.insert({'pk1' => 'partition 3', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2'})
              expected_results = [
                  {'pk1' => 'partition 3', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2', 'field1' => nil},
                  {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 3', 'field1' => nil},
              ]
              expect(subject.select('*')).to eq(expected_results)
            end
          end

          context 'when the partition key is in order, by the clustering columns are not' do
            it 'should sort by partition key, then by clustering columns' do
              subject.insert({'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2'})
              expected_results = [
                  {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2', 'field1' => nil},
                  {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 3', 'field1' => nil},
              ]
              expect(subject.select('*')).to eq(expected_results)
            end
          end

          context 'when the first clustering column is in order, but the second is not' do
            it 'should sort by partition key, then by clustering columns' do
              subject.insert({'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 2'})
              expected_results = [
                  {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 2', 'field1' => nil},
                  {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 3', 'field1' => nil},
              ]
              expect(subject.select('*')).to eq(expected_results)
            end
          end

          context 'when specifying a different order' do
            it 'should sort by partition key, then by clustering columns in the specified order' do
              subject.insert({'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 2'})
              expected_results = [
                  {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 3', 'field1' => nil},
                  {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 2', 'field1' => nil},
              ]
              expect(subject.select('*', order: {'ck1' => :desc, 'ck2' => :desc})).to eq(expected_results)
            end

            context 'with a different ordering' do
              it 'should sort by partition key, then by clustering columns in the specified order' do
                subject.insert({'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 2'})
                expected_results = [
                    {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 2', 'field1' => nil},
                    {'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 3', 'field1' => nil},
                ]
                expect(subject.select('*', order: {'ck1' => :asc, 'ck2' => :asc})).to eq(expected_results)
              end
            end

            describe 'an invalid range query on partition key' do
              let(:pk_part_one) { Cassandra::Column.new('pk1', 'string', :asc) }
              let(:pk_part_two) { Cassandra::Column.new('pk2', 'string', :asc) }
              let(:partition_key) { [pk_part_one, pk_part_two] }

              it 'should raise an error indicating the invalid partition part used in the range query' do
                expect { subject.select('*', restriction: {'pk1' => %w(hello world), 'pk2' => 'goodbye world'}) }.to raise_error(Cassandra::Errors::InvalidError, 'Partition KEY part pk1 cannot be restricted by IN relation (only the last part of the partition key can)')
              end
            end

            describe 'an invalid ordering clause' do
              before do
                subject.insert({'pk1' => 'partition 3', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 3', 'ck2' => 'additional clustering 2'})
              end

              describe 'ordering by an invalid column' do
                it 'should raise an error if ordering by a partition key column' do
                  expect { subject.select('*', order: {'pk1' => :asc}) }.to raise_error(Cassandra::Errors::InvalidError, 'Order by is currently only supported on the clustered columns of the PRIMARY KEY, got pk1')
                end

                it 'should raise an error if ordering by a non-existent column' do
                  expect { subject.select('*', order: {'ckkkkkk1' => :desc}) }.to raise_error(Cassandra::Errors::InvalidError, 'Order by on unknown column(s) ckkkkkk1')
                end
              end

              context 'when columns are provided in the wrong order' do
                it 'should raise an error when missing part of the ordering key' do
                  expect { subject.select('*', order: {'ck2' => :asc}) }.to raise_error(Cassandra::Errors::InvalidError, 'Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY (expected ck1, ck2 got ck2)')
                end

                it 'should raise an error when in the wrong order' do
                  expect { subject.select('*', order: {'ck2' => :asc, 'ck1' => :asc}) }.to raise_error(Cassandra::Errors::InvalidError, 'Order by currently only support the ordering of columns following their declared order in the PRIMARY KEY (expected ck1, ck2 got ck2, ck1)')
                end
              end

              context 'with inconsistent ordering directions' do
                it 'should raise an error' do
                  expect { subject.select('*', order: {'ck1' => :asc, 'ck2' => :desc}) }.to raise_error(Cassandra::Errors::InvalidError, 'Ordering direction must be consistent across all clustering columns')
                end
              end
            end

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
          let(:list_of_attributes) do
            (1..2).map do |pk1|
              (1..3).map do |pk2|
                (1..2).map do |ck1|
                  (1..2).map do |ck2|
                    {
                        'pk1' => "partition #{pk1}",
                        'pk2' => "additional partition #{pk2}",
                        'ck1' => "clustering #{ck1}",
                        'ck2' => "additional clustering #{ck2}",
                    }
                  end
                end
              end
            end.flatten
          end

          before do
            list_of_attributes.each do |attributes|
              subject.insert(attributes)
            end
          end

          context 'when the filter is empty' do
            it 'should treat it as having no filter' do
              expect(subject.select('*', restriction: {})).to eq(subject.rows)
            end
          end

          describe 'filtering by partition key' do
            it 'should return all records for that partition' do
              expected_results = [{'pk1' => 'partition', 'pk2' => 'additional partition', 'ck1' => 'clustering', 'ck2' => 'additional clustering', 'field1' => nil}]
              expect(subject.select('*', restriction: {'pk1' => 'partition', 'pk2' => 'additional partition'})).to eq(expected_results)
            end

            context 'with a different partition key' do
              it 'should return all records for that partition' do
                expected_results = [
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 1', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2', 'field1' => nil},
                ]
                expect(subject.select('*', restriction: {'pk1' => 'partition 2', 'pk2' => 'additional partition 2'})).to eq(expected_results)
              end
            end

            context 'when the partition key is IN restricted' do
              it 'should return all records for that partition range' do
                expected_results = [
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 1', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 1', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 1', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 1', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 1', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 1', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 3', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2', 'field1' => nil},
                ]
                expect(subject.select('*', restriction: {'pk1' => 'partition 2', 'pk2' => ['additional partition 1', 'additional partition 3']})).to eq(expected_results)
              end
            end

            it 'should raise an error if only specifying part of the partition key' do
              expect { (subject.select('*', restriction: {'pk1' => 'partition'})) }.to raise_error(Cassandra::Errors::InvalidError, 'Missing partition key part(s) "pk2"')
            end

            context 'with a different part missing' do
              it 'should raise an error' do
                expect { (subject.select('*', restriction: {'pk2' => 'additional partition'})) }.to raise_error(Cassandra::Errors::InvalidError, 'Missing partition key part(s) "pk1"')
              end
            end

            context 'with both parts missing' do
              it 'should raise an error' do
                expect { (subject.select('*', restriction: {'ck1' => 'clustering'})) }.to raise_error(Cassandra::Errors::InvalidError, 'Missing partition key part(s) "pk1", "pk2"')
              end
            end
          end

          describe 'filtering by clustering columns' do
            it 'should return all records for that partition, matching the specified clustering columns' do
              expected_results = [
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1', 'field1' => nil},
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2', 'field1' => nil},
              ]
              expect(subject.select('*', restriction: {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1'})).to eq(expected_results)
            end

            it 'supports clustering columns specified out of order' do
              expected_results = [
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1', 'field1' => nil},
              ]
              expect(subject.select('*', restriction: {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck2' => 'additional clustering 1', 'ck1' => 'clustering 1'})).to eq(expected_results)
            end

            context 'with a comparitor' do
              it 'should return all records for that partition, matching the specified clustering columns' do
                expected_results = [
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 1', 'field1' => nil},
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2', 'field1' => nil},
                ]
                comparitor = Statement::Comparitor.new(:ge, 'ck1', 'clustering 1')
                expect(subject.select('*', restriction: {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => comparitor})).to eq(expected_results)
              end

              context 'with a different comparison' do
                it 'should return all records for that partition, matching the specified clustering columns' do
                  expected_results = [
                      {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1', 'field1' => nil},
                      {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2', 'field1' => nil},
                  ]
                  comparitor = Statement::Comparitor.new(:lt, 'ck1', 'clustering 2')
                  expect(subject.select('*', restriction: {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => comparitor})).to eq(expected_results)
                end
              end

              context 'with a comparitor comparing multiple columns' do
                let(:comparer_keys) { %w(ck1 ck2) }
                let(:comparer_values) { ['clustering 1', 'additional clustering 1'] }
                let(:comparitor) { Statement::Comparitor.new(:ge, comparer_keys, comparer_values) }
                let(:restriction) { {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', comparer_keys => comparitor} }
                let(:expected_results) do
                  [
                      {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1', 'field1' => nil},
                      {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2', 'field1' => nil},
                      {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 1', 'field1' => nil},
                      {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2', 'field1' => nil},
                  ]
                end

                it 'should return all records for that partition, matching the specified clustering columns' do
                  expect(subject.select('*', restriction: restriction)).to eq(expected_results)
                end
              end
            end

            it 'should raise an error if earlier clustering keys are not restricted' do
              expect do
                subject.select('*', restriction: {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck2' => 'additional clustering 1'})
              end.to raise_error(Cassandra::Errors::InvalidError, 'Clustering key part(s) "ck1" must be restricted')
            end

            context 'with a different set of clustering columns' do
              let(:attributes) do
                {'pk1' => 'other partition',
                 'cluster1' => 'clustering',
                 'cluster2' => 'additional clustering 1',
                 'cluster3' => 'additional clustering 2'}
              end
              let(:list_of_attributes) { [] }

              let(:ck_part_one) { Cassandra::Column.new('cluster1', 'string', :desc) }
              let(:ck_part_two) { Cassandra::Column.new('cluster2', 'string', :desc) }
              let(:ck_part_three) { Cassandra::Column.new('cluster3', 'string', :desc) }
              let(:partition_key) { [pk_part_one] }
              let(:clustering_key) { [ck_part_one, ck_part_two, ck_part_three] }

              it 'should raise an error if earlier clustering keys are not restricted' do
                expect do
                  subject.select('*', restriction: {'pk1' => 'partition 2', 'cluster3' => 'additional clustering 1'})
                end.to raise_error(Cassandra::Errors::InvalidError, 'Clustering key part(s) "cluster1", "cluster2" must be restricted')
              end
            end

            context 'with multiple clustering columns specified' do
              it 'should return all records for that partition, matching the specified clustering columns' do
                expected_results = [
                    {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1', 'field1' => nil},
                ]
                expect(subject.select('*', restriction: {'pk1' => 'partition 2',
                                                         'pk2' => 'additional partition 2',
                                                         'ck1' => 'clustering 1',
                                                         'ck2' => 'additional clustering 1'})).to eq(expected_results)
              end
            end

            context 'when using multiple Comparitors with a single key' do
              let(:list_of_attributes) do
                [
                    {'pk1' => 'part', 'pk2' => 'part', 'ck1' => 'a', 'ck2' => 'z'},
                    {'pk1' => 'part', 'pk2' => 'part', 'ck1' => 'b', 'ck2' => 'z'},
                    {'pk1' => 'part', 'pk2' => 'part', 'ck1' => 'c', 'ck2' => 'z'},
                ]
              end
              let(:filter) do
                {'pk1' => 'part', 'pk2' => 'part', 'ck1' => [Statement::Comparitor.new(:gt, 'ck1', 'a'), Statement::Comparitor.new(:lt, 'ck1', 'c')]}
              end
              let(:expected_results) { [{'pk1' => 'part', 'pk2' => 'part', 'ck1' => 'b', 'ck2' => 'z', 'field1' => nil}] }

              it 'should return the row that lies in between the comparisons' do
                expect(subject.select('*', restriction: filter)).to eq(expected_results)
              end
            end

          end

          describe 'filtering by fields' do
            let(:filter) do
              {'pk1' => 'part', 'pk2' => 'part', 'field1' => 'some value'}
            end

            it 'should raise an error indicating that this is not supported' do
              expect { subject.select('*', restriction: filter) }.to raise_error(Cassandra::Errors::InvalidError, 'Filtering by fields is not supported')
            end
          end
        end
      end

      describe '#delete' do
        let(:attributes) do
          {'pk1' => 'partition',
           'pk2' => 'additional partition',
           'ck1' => 'clustering',
           'ck2' => 'additional clustering'}
        end
        let(:partition_key) { [pk_part_one, pk_part_two] }
        let(:clustering_key) { [ck_part_one, ck_part_two] }
        let(:list_of_attributes) do
          (1..2).map do |pk1|
            (1..2).map do |pk2|
              (1..2).map do |ck1|
                (1..2).map do |ck2|
                  {
                      'pk1' => "partition #{pk1}",
                      'pk2' => "additional partition #{pk2}",
                      'ck1' => "clustering #{ck1}",
                      'ck2' => "additional clustering #{ck2}",
                  }
                end
              end
            end
          end.flatten
        end

        before do
          subject.insert(attributes)
          list_of_attributes.each do |attributes|
            subject.insert(attributes)
          end
        end

        describe 'filtering by partition key' do
          it 'should return all records for that partition' do
            expected_results = [{'pk1' => 'partition', 'pk2' => 'additional partition', 'ck1' => 'clustering', 'ck2' => 'additional clustering'}]
            subject.delete({'pk1' => 'partition', 'pk2' => 'additional partition'})
            expect(subject.rows).not_to include(*expected_results)
          end

          context 'with a different partition key' do
            it 'should return all records for that partition' do
              expected_results = [
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1'},
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2'},
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 1'},
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 2', 'ck2' => 'additional clustering 2'},
              ]
              subject.delete({'pk1' => 'partition 2', 'pk2' => 'additional partition 2'})
              expect(subject.rows).not_to include(*expected_results)
            end
          end

          it 'should raise an error if only specifying part of the partition key' do
            expect { (subject.delete({'pk1' => 'partition'})) }.to raise_error(Cassandra::Errors::InvalidError, 'Missing partition key part(s) "pk2"')
          end

          context 'with a different part missing' do
            it 'should raise an error' do
              expect { (subject.delete({'pk2' => 'additional partition'})) }.to raise_error(Cassandra::Errors::InvalidError, 'Missing partition key part(s) "pk1"')
            end
          end

          context 'with both parts missing' do
            it 'should raise an error' do
              expect { (subject.delete({'ck1' => 'clustering'})) }.to raise_error(Cassandra::Errors::InvalidError, 'Missing partition key part(s) "pk1", "pk2"')
            end
          end
        end

        describe 'filtering by clustering columns' do
          it 'should return all records for that partition, matching the specified clustering columns' do
            expected_results = [
                {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1'},
                {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 2'},
            ]
            subject.delete({'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1'})
            expect(subject.rows).not_to include(*expected_results)
          end

          it 'should raise an error if earlier clustering keys are not restricted' do
            expect do
              subject.delete({'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck2' => 'additional clustering 1'})
            end.to raise_error(Cassandra::Errors::InvalidError, 'Clustering key part(s) "ck1" must be restricted')
          end

          context 'with a different set of clustering columns' do
            let(:attributes) do
              {'pk1' => 'other partition',
               'cluster1' => 'clustering',
               'cluster2' => 'additional clustering 1',
               'cluster3' => 'additional clustering 2'}
            end
            let(:list_of_attributes) { [] }

            let(:ck_part_one) { Cassandra::Column.new('cluster1', 'string', :desc) }
            let(:ck_part_two) { Cassandra::Column.new('cluster2', 'string', :desc) }
            let(:ck_part_three) { Cassandra::Column.new('cluster3', 'string', :desc) }
            let(:partition_key) { [pk_part_one] }
            let(:clustering_key) { [ck_part_one, ck_part_two, ck_part_three] }

            it 'should raise an error if earlier clustering keys are not restricted' do
              expect do
                subject.delete({'pk1' => 'partition 2', 'cluster3' => 'additional clustering 1'})
              end.to raise_error(Cassandra::Errors::InvalidError, 'Clustering key part(s) "cluster1", "cluster2" must be restricted')
            end
          end

          context 'with multiple clustering columns specified' do
            it 'should return all records for that partition, matching the specified clustering columns' do
              expected_results = [
                  {'pk1' => 'partition 2', 'pk2' => 'additional partition 2', 'ck1' => 'clustering 1', 'ck2' => 'additional clustering 1'},
              ]
              subject.delete({'pk1' => 'partition 2',
                              'pk2' => 'additional partition 2',
                              'ck1' => 'clustering 1',
                              'ck2' => 'additional clustering 1'})
              expect(subject.rows).not_to include(*expected_results)
            end
          end
        end

      end

    end
  end
end
