require 'rspec'

module Cassandra
  module Mocks
    class Statement
      describe Token do
        let(:type) { :insert }
        let(:value) { 'INSERT' }

        subject { Token.new(type, value) }

        describe 'attributes' do
          its(:type) { is_expected.to eq(:insert) }
          its(:value) { is_expected.to eq('INSERT') }

          context 'with a different type set' do
            let(:type) { :select }
            let(:value) { 'SELECT' }

            its(:type) { is_expected.to eq(:select) }
            its(:value) { is_expected.to eq('SELECT') }
          end
        end

        describe '#normalized_value' do
          context 'when a string' do
            let(:type) { :string }
            let(:value) { 'hello world' }
            its(:normalized_value) { is_expected.to eq('hello world') }
          end

          context 'when a name' do
            let(:type) { :name }
            let(:value) { 'hello world' }
            its(:normalized_value) { is_expected.to eq('hello world') }
          end

          context 'when an id' do
            let(:type) { :id }
            let(:value) { 'hello world' }
            its(:normalized_value) { is_expected.to eq('hello world') }
          end

          context 'when an integer' do
            let(:type) { :int }
            let(:value) { '5' }
            its(:normalized_value) { is_expected.to eq(5) }
          end

          context 'when a float' do
            let(:type) { :float }
            let(:value) { '5.367' }
            its(:normalized_value) { is_expected.to eq(5.367) }
          end
        end

        describe 'type querying' do
          let(:type) { :float }

          it 'should be able to query directly for the type' do
            expect(subject).to be_float
          end

          context 'with a different type' do
            let(:type) { :int }

            it 'should be able to query directly for the type' do
              expect(subject).not_to be_float
            end
          end

          context 'when querying for a different type' do
            let(:type) { :string }

            it 'should be able to query directly for the type' do
              expect(subject).to be_string
            end
          end
        end

      end
    end
  end
end
