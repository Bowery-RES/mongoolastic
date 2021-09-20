const get = require("lodash.get");
const utils = require("../utils");

const UPDATE_DEPENDENCIES_SCRIPT = `
int findIndex(def items, def newDocPart) {
    for (def i = 0; i < items.length; i++) {
        if (items[i]._id == newDocPart._id) {
            return i;
        }
    }

    return -1;
  }

  void updateDocByPath(def doc, def pathParts, def params) {
    for (pathPart in pathParts) {
        def value = doc[pathPart];
        List newPathParts = new ArrayList(pathParts.asList());
        newPathParts.remove(0);

        if (value instanceof List) {
            if (newPathParts.length == 0) {
                int index = findIndex(value, params.newDocPart);
                if (index != -1) {
                    if (params['opType'] == 'remove') {
                        value.remove(index);
                    }
                    if (params['opType'] == 'update') {
                        value.set(index, params['newDocPart']);
                    }
                }
            } else {
                for (item in value) {
                    updateDocByPath(item, newPathParts, params);
                }
            }
        } else if (value instanceof Object) {
            updateDocByPath(value, newPathParts, params);

            if (newPathParts.length == 0) {
                if (value._id == params.newDocPart._id) {
                    if (params['opType'] == 'remove') {
                        doc[pathPart] = null;
                    }
                    if (params['opType'] == 'update') {
                        doc[pathPart] = params['newDocPart'];
                    }
                }
            }
        }
    }
  }

  updateDocByPath(ctx._source, params.pathParts, params);
`;

const getUpdateQuery = (index, newDocPartId, fieldPath, mappingSchema) => {
  const buildQueryRecursively = (
    currentPath,
    fieldPathParts,
    mappingSchema
  ) => {
    const pathPart = fieldPathParts[0];

    if (!pathPart) {
      return {
        term: {
          [`${fieldPath}._id`]: newDocPartId,
        },
      };
    }

    currentPath.push(pathPart);

    let query = null;
    if (mappingSchema[pathPart].properties) {
      query = buildQueryRecursively(
        currentPath.splice(-1),
        fieldPathParts.splice(1),
        mappingSchema[pathPart].properties
      );

      currentPath.push(pathPart);

      if (mappingSchema[pathPart].type === "nested") {
        return {
          nested: {
            path: currentPath.join("."),
            query,
          },
        };
      }
    }

    return query;
  };

  const query = buildQueryRecursively(
    [],
    fieldPath.split("."),
    mappingSchema[index].mappings.properties
  );
  return query;
};

const syncDependentIndexField = async ({
  indexName,
  mappingSchema,
  fieldPath,
  sourceDoc,
  opType,
  client,
}) => {
  const mappingFieldPath = utils.convertToMappingSchemaPath(
    indexName,
    fieldPath
  );
  const newDocPart = utils.serialize(
    sourceDoc,
    get(mappingSchema, mappingFieldPath)
  );

  const newDocPartId = newDocPart._id;

  const fieldPathParts = fieldPath.split(".");

  await client.updateByQuery({
    index: indexName,
    body: {
      script: {
        source: UPDATE_DEPENDENCIES_SCRIPT,
        lang: "painless",
        params: {
          newDocPart,
          pathParts: fieldPathParts,
          opType,
        },
      },
      query: getUpdateQuery(indexName, newDocPartId, fieldPath, mappingSchema),
    },
  });
};

/**
 * Synchronize dependent indexes data
 * @param {Object} [sourceDoc]
 * @param {Object} [esOptions]
 * @param {Object} [opType]
 * @returns {Promise}
 */

async function syncDependantIndexes(sourceDoc, esOptions, opType) {
  const dependantIndexes = esOptions.dependantIndexes;

  return Promise.all(
    dependantIndexes.map(async ({ name, fields }) => {
      const mappingSchema = await esOptions.client.indices.getMapping({
        index: name,
      });
      return Promise.all(
        fields.map((field) =>
          syncDependentIndexField({
            indexName: name,
            mappingSchema,
            fieldPath: field,
            sourceDoc,
            opType,
            client: esOptions.client,
          })
        )
      );
    })
  );
}

module.exports = syncDependantIndexes;
