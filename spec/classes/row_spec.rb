require 'rspec'

module Cassandra
  module Mocks
    describe Row do

      let(:row) { Row.new }

      describe '#insert_record' do
        let(:clustering_columns) { %w(ck1) }
        let(:record_values) { Faker::Lorem.words }
        let(:check_exists) { false }
        let(:do_insert) { row.insert_record(clustering_columns, record_values, check_exists) }

        subject { do_insert }

        it { is_expected.to eq(true) }

        it 'should insert the record, clustered by the provided clustering columns' do
          do_insert
          expect(row.clusters).to eq('ck1' => Record.new(record_values))
        end

        context 'when the record already exists' do
          let(:record_values_two) { Faker::Lorem.words }
          let(:do_insert_two) { row.insert_record(clustering_columns, record_values_two, check_exists) }

          subject { do_insert_two }

          before { do_insert }

          it { is_expected.to eq(true) }

          it 'should keep only the latest version' do
            do_insert
            do_insert_two
            expect(row.clusters).to eq('ck1' => Record.new(record_values_two))
          end

          context 'when check_exists is set to true' do
            let(:check_exists) { true }

            it { is_expected.to eq(false) }

            it 'should keep the older version' do
              do_insert
              do_insert_two
              expect(row.clusters).to eq('ck1' => Record.new(record_values))
            end
          end
        end

        context 'with multiple clustering keys' do
          let(:clustering_columns) { %w(ck1 ck2) }

          it 'should insert the record, clustered by the provided clustering columns' do
            do_insert
            expect(row.clusters).to eq('ck1' => {'ck2' => Record.new(record_values)})
          end

          context 'with multiple records having different clustering columns' do
            let(:clustering_columns_two) { %w(ck1 ck3) }
            let(:record_values_two) { Faker::Lorem.words }
            let(:do_insert_two) { row.insert_record(clustering_columns_two, record_values_two, check_exists) }

            it 'should insert the record, clustered by the provided clustering columns' do
              do_insert
              do_insert_two
              expect(row.clusters).to eq('ck1' => {
                  'ck2' => Record.new(record_values),
                  'ck3' => Record.new(record_values_two)
              })
            end
          end
        end
      end

    end
  end
end