from core.receipts.application.streams.receipt_notifier import notify


def notify_transactions_persisted() -> None:
    notify()
