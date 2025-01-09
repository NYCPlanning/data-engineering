from typing import Any
from dataclasses import dataclass

from dcpy.models.connectors import ConnectorDispatcher


# Define Some Animals
@dataclass
class Animal:
    age: int


@dataclass
class Dog(Animal):
    name: str

    def bark(self):
        print(f"bark {self.name}")


class Beagle(Dog):
    pass


@dataclass
class Cat(Animal):
    remaining_lives: int = 9

    def glare(self):
        pass


# Define Some Connectors to handle those animals
@dataclass
class AnimalConn:
    conn_type: str

    def push(self, _: Animal) -> Any:
        """push"""

    def pull(self, _: Animal) -> Any:
        """pull"""


@dataclass
class DogConn:
    conn_type: str

    def push(self, dog: Dog) -> Any:
        dog.bark()

    def pull(self, _: Dog) -> Any:
        """pull"""


@dataclass
class BeagleConn:
    conn_type: str

    def push(self, dog: Beagle) -> Any:
        dog.bark()

    def pull(self, _: Beagle) -> Any:
        """pull"""


@dataclass
class CatConn:
    conn_type: str

    def push(self, cat: Cat) -> Any:
        cat.glare()

    def pull(self, _: Cat) -> Any:
        """pull"""


def test_dog_contravariance():
    dog_dispatcher = ConnectorDispatcher[Dog, Dog]()

    # This is fine! Callables for Animal can also handle a Dog.
    dog_dispatcher.register(conn_type="animal", connector=AnimalConn("animal"))
    # Also fine. Obviously dogs are dogs.
    dog_dispatcher.register(conn_type="dog", connector=DogConn("dog"))

    def things_that_fail_the_type_checker_if_you_remove_the_type_ignore():
        """Chucking these in a function to 1) not execute them, 2) indent them"""

        # mypy won't allow this because the CatConn might be passed a dog, and dogs can't meow(), for example
        dog_dispatcher.register(conn_type="cat", connector=CatConn("Soxx"))  # type: ignore

        # This one is less obvious. Due to contravariance, subclasses of Dog are not allowed
        # for the dog_dispatcher, which takes `Dog` types for the push and pull method.
        # This is somewhat counterintuitive, but allowing this in generics breaks type-safety
        dog_dispatcher.register(conn_type="beagle", connector=BeagleConn("snoopy"))  # type: ignore

    ### On to the dispatching

    # This is fine! Any function that can handle an Animal can handle Dog
    dog_dispatcher.push("animal", Dog(age=4, name="rufus"))

    def more_things_that_would_fail_the_type_checker():
        # Fails for obvious reasons
        dog_dispatcher.push("cat", Cat(age=4))  # type: ignore

        # This would execute just fine, but given then dynamic nature of dispatch
        # mypy can't be sure that an Animal won't be passed to a DogConnector
        dog_dispatcher.push("animal", Animal(age=4))  # type: ignore

        # This would actually break though, after we called .bark() on the Animal.
        dog_dispatcher.push("dog", Animal(age=4))  # type: ignore

        # This should actually work just fine, but due to contraviariance is prohibited.
        #
        dog_dispatcher.push("dog", Beagle(age=4))  # type: ignore


# Example of covariance. Works fine and type-checker is happy.
# def run(animal: Animal):
#     pass

# run(Beagle(age=4, name="snoopy"))
