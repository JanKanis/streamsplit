Streamsplit
===========


This is a command line tool to split a data stream into multiple streams, and merge those streams together again to recrate the original stream.

The original reason for this program was to speed up data transfer over SSH where the machines don't have hardware encryption support but do have a fast network connection and multiple cpu cores. SSH is not multi-threaded, and doesn't plan to add that for fear introducing security issues. In such cases, the SSH encryption can become the limiting factor in transferring data over SSH. `streamsplit` can help speed up the transfer by splitting the datastream into multiple streams, each of which can be sent over an independent SSH channel, and they can be recombined at the destination.

Example usage:

Test transfer speed by sending data from `/dev/zero`. Use `pv` on `remoteserver` to measure the speed:

	streamsplit -v split 2 -i /dev/zero 'ssh remoteserver streamsplit -v merge "pv -f >/dev/null"'

File transfer:

	streamsplit -i ./mybigfile --split=2 'ssh remoteserver streamsplit --merge -o ./destinationfile dummyargument

(`dummyargument` is needed due to a bug, the command line parsing still needs some work)


License
-------

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

A copy of the GNU General Public License can be found in the file
`gpl-3.0.txt`, or you can find it at https://www.gnu.org/licenses/.
