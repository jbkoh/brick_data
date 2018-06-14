# Brick Federation
This project is to federate different data models necessary for various applications/systems in buildings with [Brick](https://brickschema.org).

- Current focus: Brick metadata, timeseries data, user location tracking and building geometry
- [A working design documentation](https://docs.google.com/document/d/11MVub8Eoz5qEVGjv7FQHVkK-qkHZNxQEWbmlm1ky4x4/edit#heading=h.jmk656olcz49)


## Prerequisite

### Installation
- [Virtuoso](https://github.com/openlink/virtuoso-opensource) as a Brick database
- [PostgreSQL](https://wiki.postgresql.org/wiki/Detailed_installation_guides) for timeseries and GIS.
    1. [PostGIS](https://postgis.net/install/) for geometries.
    2. [TimescaleDB](https://docs.timescale.com/v0.9/getting-started/installation) for timeseries.

### Tested Environments
- Ubuntu 16.04



## Examples

### Case 1
- Scenario: A person walks from a point to another linearly and there is a room. Check if the person is in the room at certain point
- [data insertion](https://github.com/jbkoh/brick-federation/blob/master/sample_data/gen_sample_gis_data.py)
- [query](https://github.com/jbkoh/brick-federation/blob/master/gis_test1.py)

# Primary Projects dependent on this
- [Brick Server](git@github.com:jbkoh/brick-server.git)
