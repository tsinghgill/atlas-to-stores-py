import logging
import sys

from turbine.src.turbine_app import RecordList, TurbineApp

logging.basicConfig(level=logging.INFO)


def transform(records: RecordList) -> RecordList:
    logging.info(f"processing {len(records)} record(s)")
    print("Inside Tranform for atlas-to-stores")
    for record in records:
        logging.info(f"input: {record}")
        print("Inside Tranform FOR LOOP for atlas-to-stores")
        try:
            record.value["payload"]["store_id"] = "002"
            record.value["payload"]["store_location"] = "west"

            record.value["payload"]["after"]["store_id"] = "002"
            record.value["payload"]["after"]["store_location"] = "west"
            
            logging.info(f"output: {record}")
        except Exception as e:
            print("Error occurred while parsing records: " + str(e))
            logging.info(f"output: {record}")
    return records


class App:
    @staticmethod
    async def run(turbine: TurbineApp):
        try:
            source = await turbine.resources("mongo-atlas")

            records = await source.records("dispensed_pills_from_west_store", {})

            # turbine.register_secrets("PWD")

            transformed = await turbine.process(records, transform)
            
            destination_db = await turbine.resources("west-store-mongo")

            await destination_db.write(transformed, "dispensed_pills_from_atlas_cloud", {
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState"
            })

        except Exception as e:
            print(e, file=sys.stderr)