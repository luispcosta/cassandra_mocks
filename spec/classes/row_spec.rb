require 'rspec'

module Cassandra
  module Mocks
    describe Row do

      let(:partition_key) { Faker::Lorem.words }
      let(:row) { Row.new(partition_key) }

      subject { row }

      describe '#partition_key' do
        its(:partition_key) { is_expected.to eq(partition_key) }
      end

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

      describe '#find_records' do
        let(:search_clustering_columns) { [] }

        subject { row.find_records(search_clustering_columns) }

        it { is_expected.to eq([]) }

        context 'with some records' do
          let(:clustering_columns) { %w(ck1) }
          let(:record_values) { Faker::Lorem.words }

          before do
            row.insert_record(clustering_columns, record_values, false)
          end

          it { is_expected.to eq([[*partition_key, 'ck1', *record_values]]) }

          context 'with a restriction' do
            let(:search_clustering_columns) { %w(ck2) }

            it { is_expected.to eq([]) }

            context 'when the restriction includes an existing record' do
              let(:search_clustering_columns) { %w(ck1) }

              it { is_expected.to eq([[*partition_key, 'ck1', *record_values]]) }
            end
          end

          context 'with multiple clustering columns' do
            let(:clustering_columns) { %w(ck1 ck2) }

            it { is_expected.to eq([[*partition_key, 'ck1', 'ck2', *record_values]]) }

            context 'with a restriction' do
              let(:search_clustering_columns) { %w(ck1 ck1) }

              it { is_expected.to eq([]) }

              context 'when the restriction includes an existing record' do
                let(:search_clustering_columns) { %w(ck1 ck2) }

                it { is_expected.to eq([[*partition_key, 'ck1', 'ck2', *record_values]]) }
              end
            end

            context 'with multiple records' do
              let(:clustering_columns_two) { %w(ck1 ck3) }
              let(:record_values_two) { Faker::Lorem.words }
              let(:result_record) { [*partition_key, 'ck1', 'ck2', *record_values] }
              let(:result_record_two) { [*partition_key, 'ck1', 'ck3', *record_values_two] }

              before do
                row.insert_record(clustering_columns_two, record_values_two, false)
              end

              it { is_expected.to eq([result_record, result_record_two]) }
            end
          end

          context 'with multiple records' do
            let(:clustering_columns_two) { %w(ck2) }
            let(:record_values_two) { Faker::Lorem.words }
            let(:result_record) { [*partition_key, 'ck1', *record_values] }
            let(:result_record_two) { [*partition_key, 'ck2', *record_values_two] }

            before do
              row.insert_record(clustering_columns_two, record_values_two, false)
            end

            it { is_expected.to eq([result_record, result_record_two]) }
          end
        end
      end

      describe '#delete_records' do
        let(:search_clustering_columns) { [] }

        subject { row.find_records(search_clustering_columns) }

        context 'with no record' do
          before { row.delete_records(search_clustering_columns) }

          it { is_expected.to be_empty }
        end

        context 'with some records' do
          let(:clustering_columns) { %w(ck1) }
          let(:record_values) { Faker::Lorem.words }

          before do
            row.insert_record(clustering_columns, record_values, false)
            row.delete_records(search_clustering_columns)
          end

          it { is_expected.to be_empty }

          context 'with a restriction' do
            let(:search_clustering_columns) { %w(ck2) }

            subject { row.find_records([]) }

            it { is_expected.to eq([[*partition_key, 'ck1', *record_values]]) }
          end
        end

        context 'with multiple records and a restriction' do
          let(:search_clustering_columns) { %w(ck2) }
          let(:clustering_columns) { %w(ck1) }
          let(:record_values) { Faker::Lorem.words }
          let(:clustering_columns_two) { %w(ck2) }
          let(:record_values_two) { Faker::Lorem.words }

          subject { row.find_records([]) }

          before do
            row.insert_record(clustering_columns, record_values, false)
            row.insert_record(clustering_columns_two, record_values_two, false)
            row.delete_records(search_clustering_columns)
          end

          it { is_expected.to eq([[*partition_key, 'ck1', *record_values]]) }
        end
      end

    end
  end
end
