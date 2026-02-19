from __future__ import annotations

from datetime import date, timedelta

from sqlalchemy import text


def test_payments_insert_update_history(db_engine):
    test_pmo = "PMO-TEST-PAY"
    payment_date = date.today()
    new_date = payment_date + timedelta(days=7)

    with db_engine.connect() as conn:
        trans = conn.begin()
        try:
            conn.execute(
                text(
                    """
                    INSERT INTO payments (project_gid, pmo_id, status, payment_date, glosa)
                    VALUES (:gid, :pmo, :status, :fecha, :glosa)
                    """
                ),
                {
                    "gid": None,
                    "pmo": test_pmo,
                    "status": "estimado",
                    "fecha": payment_date,
                    "glosa": "test pago",
                },
            )
            pid = conn.execute(
                text("SELECT id FROM payments WHERE pmo_id = :pmo ORDER BY id DESC LIMIT 1"),
                {"pmo": test_pmo},
            ).scalar()
            assert pid is not None

            conn.execute(
                text(
                    """
                    INSERT INTO payment_estimate_history (payment_id, old_date, new_date)
                    VALUES (:pid, :old, :new)
                    """
                ),
                {"pid": pid, "old": payment_date, "new": new_date},
            )
            conn.execute(
                text(
                    """
                    UPDATE payments
                    SET payment_date = :new_date, glosa = :glosa
                    WHERE id = :pid
                    """
                ),
                {"pid": pid, "new_date": new_date, "glosa": "test pago actualizado"},
            )

            history_count = conn.execute(
                text("SELECT COUNT(*) FROM payment_estimate_history WHERE payment_id = :pid"),
                {"pid": pid},
            ).scalar()
            assert history_count == 1

            current_date = conn.execute(
                text("SELECT payment_date FROM payments WHERE id = :pid"),
                {"pid": pid},
            ).scalar()
            assert current_date == new_date
        finally:
            trans.rollback()
