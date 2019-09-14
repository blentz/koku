# Generated by Django 2.2.4 on 2019-09-05 19:20

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('cost_models', '0011_costmodelaudit'),
    ]

    operations = [
        migrations.RunSQL(
            """
            CREATE OR REPLACE FUNCTION process_cost_model_audit() RETURNS TRIGGER AS $cost_model_audit$
                DECLARE
                    provider_uuids uuid[];
                BEGIN
                    --
                    -- Create a row in cost_model_audit to reflect the operation performed on cost_model,
                    -- make use of the special variable TG_OP to work out the operation.
                    --
                    IF (TG_OP = 'DELETE') THEN
                        provider_uuids := (SELECT array_agg(provider_uuid) FROM cost_model_map WHERE cost_model_id = OLD.uuid);
                        INSERT INTO cost_model_audit SELECT nextval('cost_model_audit_id_seq'), 'DELETE', now(), provider_uuids, OLD.*;
                        RETURN OLD;
                    ELSIF (TG_OP = 'UPDATE') THEN
                        provider_uuids := (SELECT array_agg(provider_uuid) FROM cost_model_map WHERE cost_model_id = NEW.uuid);
                        INSERT INTO cost_model_audit SELECT nextval('cost_model_audit_id_seq'), 'UPDATE', now(), provider_uuids, NEW.*;
                        RETURN NEW;
                    ELSIF (TG_OP = 'INSERT') THEN
                        provider_uuids := (SELECT array_agg(provider_uuid) FROM cost_model_map WHERE cost_model_id = NEW.uuid);
                        INSERT INTO cost_model_audit SELECT nextval('cost_model_audit_id_seq'), 'INSERT', now(), provider_uuids, NEW.*;
                        RETURN NEW;
                    END IF;
                    RETURN NULL; -- result is ignored since this is an AFTER trigger
                END;
            $cost_model_audit$ LANGUAGE plpgsql;

            CREATE TRIGGER cost_model_audit
            AFTER INSERT OR UPDATE OR DELETE ON cost_model
                FOR EACH ROW EXECUTE PROCEDURE process_cost_model_audit();
            """
        )
    ]