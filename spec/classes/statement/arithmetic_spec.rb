require 'rspec'

module Cassandra
  module Mocks
    class Statement
      describe Arithmetic do
        let(:operation) { :plus }
        let(:column) { 'field1' }
        let(:amount) { 1 }

        subject { Arithmetic.new(operation, column, amount) }

        describe '#apply!' do
          let(:row) { {'field1' => 15, 'field2' => 27} }

          it 'should return the row with the operation applied on the specified row' do
            expect(subject.apply!(row)).to eq({'field1' => 16, 'field2' => 27})
          end

          context 'with a different column' do
            let(:column) { 'field2' }

            it 'should update that column' do
              expect(subject.apply!(row)).to eq({'field1' => 15, 'field2' => 28})
            end
          end

          context 'when the column does not exist' do
            let(:row) { {'field2' => 27} }

            it 'should assume a default value of 0' do
              expect(subject.apply!(row)).to eq({'field1' => 1, 'field2' => 27})
            end
          end

          context 'with a different amount' do
            let(:amount) { 5 }

            it 'should update that column by the specified amount' do
              expect(subject.apply!(row)).to eq({'field1' => 20, 'field2' => 27})
            end
          end

          context 'with a different operation' do
            let(:operation) { :minus }

            it 'should update that column using the specified operation' do
              expect(subject.apply!(row)).to eq({'field1' => 14, 'field2' => 27})
            end
          end
        end

      end
    end
  end
end
