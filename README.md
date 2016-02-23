# Buffy

Welcome to Buffy. 

Currently everything that we want version for the project is stored in this
monorepo. Why a monorepo? At the moment, we are regularly making changes cut
across multiple sub-projects. It makes sense to keep them all together in a
single monorepo at this time.

## Naming

All project names are pulled from characters from the TV show _Buffy the Vampire
Slayer_. You don't have to have watched the show to work on the project, but you
have to have watched in order to name anything. We jokingly take the naming
seriously and try to name components after a character whose role on the show
matches their role in this project.

## Important Document Highlights

Before commiting any code, be sure to read the [contributing](CONTRIBUTING.md)
documentation.

Check out the [build](BUILD.md) documentation for important information on how
we stay up to date with the latest Pony compiler changes.

Additional documentation, including onboarding is in the [doc](doc/)
directory.
 
## Components

* Buffy: Stream processing core
* Dagon: Responsible for setting up all components and running tests
* Erbau: Short term "build tool"
* Giles: Part of our external verification system for Buffy
* Spike: Destructive testing. Takes a smoothly functioning Buffy and causes
Chaos.
