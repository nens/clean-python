# clean-python

[![Tests](https://github.com/nens/clean-python/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/nens/clean-python/actions/workflows/test.yml)

``clean-python`` contains abstractions for *clean architecture* in Python

It is independent of frameworks and has asyncio at its core.

The terminology used is consistently derived from the "Big Blue Book" (Domain Driven Design by E. Evans, 2004). Software consists of one or more modules, each having four layers: presentation, application, domain, and infrastructure. Each layer has its own responsibilities, in short:

- presentation: show information to the user and interpret the user's commands.
- application: implement use cases that direct the domain objects.
- domain: all domain concepts and rules; this layer is the heart of the software.
- infrastructure: generic capabilities that support the higher layers

A big inspiration for this was the ``easy`` typescript framework by S. Hoogendoorn and others
(https://github.com/thisisagile/easy).

## Motivation

The main goals of using layered architecture is isolating the domain-specific concepts from
other functions related only to software technology. In this way:

- The knowledge embedded in the domain is distilled and can more easily be
  understood and changed.
- Developers are able to quickly grasp the code base because it uses
  a consistent structure and naming system.
- Depenencies are reduced resulting in a higher maintainability.
- Unittests can be made more easily (increasing reliability).

## Dependencies
Layers are loosly coupled with dependencies in only one direction: presentation > application > infrastructure > domain. In other words: the number of dependencies of the software's core business are as limited as possible.

A module may only depend on another module though its infrastructure layer. See ``InternalGateway``.

This library was initially developed as a web backend using FastAPI. Its core dependency is ``pydantic``,
for strict type parsing and validation. Optional dependencies may be added as needed.

## Core concepts

### Domain Layer

The domain layer is where the model lives. The domain model is a set of concepts; the domain layer
is the manifestation of that model. Concepts in the domain model must have a 1:1 representation in the
code and vice versa.

THe layer does not depend on all other layers. Interaction with the infrastructure layer may be done
using dependency injection from the application layer. It is allowable to have runtime dependencies on the
infrastructure layer to set for instance default ``Gateway`` implementations.

There are 5 kinds of objects in this layer:

- *Entity*: Types that have an identity (all attributes of an instance may change- but the instance is still the same)
  Entities have an ``id`` and default fields associated with state changes ()``created_at``, ``updated_at``).
- *ValueObject*: Types that have no identity (these are just complex values like a datetime).
- *DomainService*: Important domain operations that aren't natural to model as objects. A service is stateless.
- *Repository*: A repository is responsible for persistence (``add`` / ``get`` / ``filter``). This needs
  a *Gateway* to interface with e.g. a database; an instance of a *Gateway* is typically injected into a
  Repository from the application layer.
- *DomainEvent*: A domain event may be emitted to signal a state change.

Associations between objects are hard. Especially many-to-many relations. We approach this by grouping objects
into *aggregates*. An aggregate is a set of objects that change together / have the same lifecycle (e.g. delete together). One entity is the aggregate root; we call this the ``RootEntity``. A ``ChildEntity`` occurs only very
rarely; mostly a nested object derive its identity from a ``RootEntity``.

All change and access goes through the repository of a ``RootEntity``. The ``RootEntity`` can be a complicated
nested object; how to map this to an SQL database is the issue of the infrastructure layer.

### Infrastructure Layer

An infrastructure layer primarily contains ``Gateway`` objects that interface with a single external resource.
The ``Gateway`` implements persistence methods to support the domain and application layers. Much of the implementation will be in frameworks or other dependencies.

The methods of a ``Gateway`` may directly return a domain object, or return a dictionary with built-in types (``Json``).

Other gateway examples are: email sending and logstash logging.

### Application layer

The application layer defines the use cases of the application. Example use cases are `create_user` or `list_user_roles`. These methods have nothing to do with a REST API or command-line interface; this is
the business of the presentation layer.

In addition to directing the domain objects, an application layer method could trigger other behavior
like logging or triggering other applications. At first, it may as well be just a single function call.

This layer is kept thin. It directs domain objects, and possibly interacts with other systems
(for instance by sending a message through the infrastructure layer). The application layer should
not contain fundamental domain rules.

### Presentation Layer

The presentation layer shows information to the user and interprets the user's commands.
Its main job is to get the application-layer use cases to be usable for an actual user.

The currently only option in ``clean-python`` is a REST API using FastAPI.

## Modules

The primary objective of compartimentalizing code into modules is to prevent cognitive overload.
The modules divide the domain layer, everything else follows. There should be low coupling
between modules and high cohesion whithin a module. Modules are first and foremost a conceptual
structure.

In Python, a module should be implemented with a single .py file or a folder of .py files (respectively
called modules and packages).

Modules have a public API (presentation layer) and encapsulate their database. Only in this way
the internal consistency can be guaranteed by the module's domain layer.

Our current approach is to have 1 *aggregate* (whose root is implemented as a ``RootEntity``) per module.

## Installation

``clean-python`` can be installed with:

    $ pip install clean-python

Optional dependencies can be added with:

    $ pip install clean-python[sql,fastapi]
