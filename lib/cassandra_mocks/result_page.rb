module Cassandra
  module Mocks
    class ResultPage < Array

      def last_page?
        true
      end

      def next_page
      end

      def next_page_async
        Cassandra::Future.value(next_page)
      end

      def paging_state
      end

      def execution_info
      end

    end
  end
end
