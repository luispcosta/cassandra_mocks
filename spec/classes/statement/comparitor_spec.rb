require 'spec_helper'

module Cassandra
  module Mocks
    class Statement
      describe Comparitor do

        let(:operation) { :lt }
        let(:column) { 'field1' }
        let(:value) { 'value1' }
        let(:comparitor) { Comparitor.new(operation, column, value) }

        subject { comparitor }

        describe '#check_against' do
          let(:row) { {'field1' => 'value2', 'field2' => 'value0'} }

          it 'should be the result of the comparison' do
            expect(subject.check_against(row)).to eq(false)
          end

          context 'with a different column' do
            let(:column) { 'field2' }

            it 'should be the result of the comparison' do
              expect(subject.check_against(row)).to eq(true)
            end
          end

          context 'with a different comparison value' do
            let(:value) { 'value75' }

            it 'should be the result of the comparison' do
              expect(subject.check_against(row)).to eq(true)
            end
          end

          context 'when the values are the same' do
            let(:value) { 'value2' }

            it 'should be the result of the comparison' do
              expect(subject.check_against(row)).to eq(false)
            end
          end

          context 'with a different operation' do
            describe 'less than or equal to' do
              let(:operation) { :le }

              it 'should be the result of the comparison' do
                expect(subject.check_against(row)).to eq(false)
              end

              context 'when the values are the same' do
                let(:value) { 'value2' }

                it 'should be the result of the comparison' do
                  expect(subject.check_against(row)).to eq(true)
                end
              end
            end

            describe 'less than or equal to' do
              let(:operation) { :eq }
              let(:value) { 'value2' }

              it 'should be the result of the comparison' do
                expect(subject.check_against(row)).to eq(true)
              end

              context 'when the value is less than the column' do
                let(:value) { 'value3' }
                it 'should be the result of the comparison' do
                  expect(subject.check_against(row)).to eq(false)
                end
              end
            end

            describe 'greater than or equal to' do
              let(:operation) { :ge }
              let(:value) { 'value99999' }

              it 'should be the result of the comparison' do
                expect(subject.check_against(row)).to eq(false)
              end

              context 'when the values are the same' do
                let(:value) { 'value2' }

                it 'should be the result of the comparison' do
                  expect(subject.check_against(row)).to eq(true)
                end
              end
            end

            describe 'greater than' do
              let(:operation) { :gt }

              it 'should be the result of the comparison' do
                expect(subject.check_against(row)).to eq(true)
              end

              context 'when the value is less than the column' do
                let(:value) { 'value2' }
                it 'should be the result of the comparison' do
                  expect(subject.check_against(row)).to eq(false)
                end
              end
            end

          end

          context 'with a multi key-value comparison' do
            let(:operation) { :gt }
            let(:row_values) { [1, 1, 1] }
            let(:row) { {%w(field1 field2 field3) => row_values} }
            let(:column) { %w(field1 field2 field3) }
            let(:value) { [1, 1, 1] }

            subject { comparitor.check_against(row) }

            it { is_expected.to eq(false) }

            context 'with a >=' do
              let(:operation) { :ge }
              it { is_expected.to eq(true) }
              context 'with a different row' do
                let(:row_values) { [1, 1, 0] }
                it { is_expected.to eq(false) }
              end
            end

            context 'with a ==' do
              let(:operation) { :eq }
              it { is_expected.to eq(true) }
              context 'with a different row' do
                let(:row_values) { [1, 1, 0] }
                it { is_expected.to eq(false) }
              end
            end

            context 'with a <=' do
              let(:operation) { :le }
              it { is_expected.to eq(true) }
              context 'with a different row' do
                let(:row_values) { [1, 1, 2] }
                it { is_expected.to eq(false) }
              end
            end

            context 'with a <' do
              let(:operation) { :lt }
              it { is_expected.to eq(false) }
              context 'with a different row' do
                let(:row_values) { [1, 1, 2] }
                it { is_expected.to eq(false) }
              end
            end
          end
        end

      end
    end
  end
end
