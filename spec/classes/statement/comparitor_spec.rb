require 'spec_helper'

module Cassandra
  module Mocks
    class Statement
      describe Comparitor do

        let(:operation) { :lt }
        let(:column) { 'field1' }
        let(:value) { 'value1' }

        subject { Comparitor.new(operation, column, value) }

        describe '#check' do
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
        end


      end
    end
  end
end
