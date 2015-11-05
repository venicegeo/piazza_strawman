#!/bin/sh 
/opt/storm/bin/storm list | grep ACTIVE | cut -d \  -f 1 | (
  while read TOPOLOGY;
  do
      /opt/storm/bin/storm deactivate "$TOPOLOGY"
      /opt/storm/bin/storm kill "$TOPOLOGY"
  done
)
sudo rm /opt/storm/deployed_piazza_topologies
