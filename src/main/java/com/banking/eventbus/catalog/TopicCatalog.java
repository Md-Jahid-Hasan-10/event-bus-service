package com.banking.eventbus.catalog;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class TopicCatalog {

    /** Three partitions let up to three consumers of one service share the work. */
    public static final int PARTITIONS = 3;

    private static final long MINUTE = 60_000L;
    private static final long HOUR = 60 * MINUTE;
    private static final long DAY = 24 * HOUR;

    private final List<TopicDefinition> topics;

    public TopicCatalog() {
        List<TopicDefinition> list = new ArrayList<>();

        addWithDeadLetter(list, TopicNames.ACCOUNT_COMMANDS, TopicKind.COMMANDS, 7 * DAY, 14 * DAY,
                "Commands to Account Service: ValidateTransfer, ValidateCashAccount");
        addWithDeadLetter(list, TopicNames.ACCOUNT_EVENTS, TopicKind.EVENTS, 7 * DAY, 14 * DAY,
                "Facts from Account Service: TransferValidated, TransferRejected, "
                        + "CashAccountValidated, CashAccountRejected, AccountOpened");
        addWithDeadLetter(list, TopicNames.FEE_COMMANDS, TopicKind.COMMANDS, 7 * DAY, 14 * DAY,
                "Commands to Charge Fee Service: CalculateFee");
        addWithDeadLetter(list, TopicNames.FEE_EVENTS, TopicKind.EVENTS, 7 * DAY, 14 * DAY,
                "Facts from Charge Fee Service: FeeCalculated");
        addWithDeadLetter(list, TopicNames.OTP_COMMANDS, TopicKind.COMMANDS, 7 * DAY, 14 * DAY,
                "Commands to OTP Service: IssueOtp");
        // The retention has to cover the delivery of the OTP, not only its
        // lifetime. An OTP is valid for five minutes, but the event has to
        // survive a notification service that is down for a while, plus the
        // retries. Ten minutes was too short: the event was gone while the
        // user was still waiting for the code, and the transfer stayed in
        // OTP_PENDING until the saga timeout fired.
        addWithDeadLetter(list, TopicNames.OTP_EVENTS, TopicKind.EVENTS, 2 * HOUR, 4 * HOUR,
                "Facts from OTP Service: OtpIssued (the code is encrypted)");
        addWithDeadLetter(list, TopicNames.CBS_COMMANDS, TopicKind.COMMANDS, 7 * DAY, 14 * DAY,
                "Commands to Core Banking: CreateLedgerAccount, CheckBalance, Withdraw, Deposit, Reversal");
        addWithDeadLetter(list, TopicNames.CBS_EVENTS, TopicKind.EVENTS, 7 * DAY, 14 * DAY,
                "Facts from Core Banking: LedgerAccountCreated, BalanceChecked, BalanceRejected, "
                        + "WithdrawCompleted, WithdrawFailed, "
                        + "DepositCompleted, DepositFailed, ReversalCompleted, ReversalFailed");
        addWithDeadLetter(list, TopicNames.TRANSFER_EVENTS, TopicKind.EVENTS, 7 * DAY, 14 * DAY,
                "Facts from Fund Transfer: TransferCompleted, TransferFailed, TransferReversed, "
                        + "ReconciliationRequired");
        addWithDeadLetter(list, TopicNames.USER_EVENTS, TopicKind.EVENTS, 7 * DAY, 14 * DAY,
                "Facts from User Service: UserRegistered, EmailVerificationRequested");
        addWithDeadLetter(list, TopicNames.NOTIFICATION_EVENTS, TopicKind.EVENTS, 7 * DAY, 14 * DAY,
                "Facts from Notification Service: OtpDeliveryFailed");

        this.topics = List.copyOf(list);
    }

    public List<TopicDefinition> all() {
        return topics;
    }

    public List<TopicDefinition> deadLetters() {
        return topics.stream()
                .filter(topic -> topic.kind() == TopicKind.DEAD_LETTER)
                .toList();
    }

    private static void addWithDeadLetter(List<TopicDefinition> list, String name, TopicKind kind,
                                          long retentionMs, long deadLetterRetentionMs,
                                          String description) {
        list.add(new TopicDefinition(name, kind, PARTITIONS, retentionMs, description));
        // The dead-letter topic needs at least as many partitions as the original topic.
        list.add(new TopicDefinition(name + TopicNames.DLT_SUFFIX, TopicKind.DEAD_LETTER, PARTITIONS,
                deadLetterRetentionMs, "Messages from " + name + " that failed after all retries"));
    }
}