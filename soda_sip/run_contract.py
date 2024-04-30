from soda.contracts.connection import Connection, SodaException
from soda.contracts.contract import Contract, ContractResult


def run():

    with Connection.from_yaml_file("./conn_contract.yml") as connection:
        contract: Contract = Contract.from_yaml_file("./bpl_libraries_contract.yml")
        contract_result: ContractResult = contract.verify(connection)


if __name__ == "__main__":
    run()
