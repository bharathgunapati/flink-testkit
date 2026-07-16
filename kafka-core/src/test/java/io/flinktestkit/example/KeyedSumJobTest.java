package io.flinktestkit.example;

import io.flinktestkit.FlinkTestExtension;
import io.flinktestkit.KafkaTopic;
import io.flinktestkit.TopicHandle;
import org.apache.flink.core.execution.JobClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(FlinkTestExtension.class)
class KeyedSumJobTest {

    @KafkaTopic(valueType = Deposit.class)
    static TopicHandle<Deposit> deposits;

    @KafkaTopic(valueType = AccountTotal.class)
    static TopicHandle<AccountTotal> totals;

    @Test
    void maintainsRunningTotalPerAccount() throws Exception {
        JobClient job = KeyedSumJob.runAsync(
            deposits.bootstrapServers(), deposits.topicName(), totals.topicName());

        try {
            deposits.produce("acct-1", new Deposit("acct-1", 10.0));
            deposits.produce("acct-2", new Deposit("acct-2", 5.0));
            deposits.produce("acct-1", new Deposit("acct-1", 3.0));

            Map<String, List<AccountTotal>> byKey = totals.awaitRecordsByKey(
                groups -> groups.getOrDefault("acct-1", List.of()).size() >= 2
                    && groups.getOrDefault("acct-2", List.of()).size() >= 1,
                Duration.ofSeconds(45));

            assertThat(byKey.get("acct-1"))
                .extracting(AccountTotal::total)
                .containsExactly(10.0, 13.0);
            assertThat(byKey.get("acct-2"))
                .extracting(AccountTotal::total)
                .containsExactly(5.0);
        } finally {
            job.cancel();
        }
    }
}
