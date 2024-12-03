#!/bin/bash

#for file in ./*.yaml; do 
#    echo $file;
#    docker run --rm -it -v $(pwd)/:/data rmlio/yarrrml-parser:latest -i /data/$file > ${file}.ttl
 
#done

docker run --rm -it -v $(pwd)/:/data rmlio/yarrrml-parser:latest -i /data/mappings/kym.media.frames.textual.enrichment.yaml >> mappings/kym.media.frames.textual.enrichment.yaml.ttl
docker run --rm -it -v $(pwd)/:/data rmlio/yarrrml-parser:latest -i /data/mappings/kym.media.frames.yaml >> mappings/kym.media.frames.yaml.ttl
docker run --rm -it -v $(pwd)/:/data rmlio/yarrrml-parser:latest -i /data/mappings/kym.parent.media.frames.yaml >> mappings/kym.parent.media.frames.yaml.ttl
docker run --rm -it -v $(pwd)/:/data rmlio/yarrrml-parser:latest -i /data/mappings/kym.children.media.frames.yaml >> mappings/kym.children.media.frames.yaml.ttl
docker run --rm -it -v $(pwd)/:/data rmlio/yarrrml-parser:latest -i /data/mappings/kym.siblings.media.frames.yaml >> mappings/kym.siblings.media.frames.yaml.ttl
docker run --rm -it -v $(pwd)/:/data rmlio/yarrrml-parser:latest -i /data/mappings/kym.types.media.frames.yaml >> mappings/kym.types.media.frames.yaml.ttl