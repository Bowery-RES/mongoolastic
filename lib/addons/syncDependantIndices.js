const get = require('lodash.get');
const utils = require('../utils');

const UPDATE_DEPENDENCIES_SCRIPT = `
  void deleteByIndexes(def items, def indexes) {
    for (def i = 0; i < indexes.length; i++) {
      items.remove(indexes[i]);
    }
  }

  boolean updateDocByPath(def doc, def pathParts, def params) {
    for (pathPart in pathParts) {
      def value = doc[pathPart];
      List newPathParts = new ArrayList(pathParts.asList());
      newPathParts.remove(0);

      boolean isTargetLevel = newPathParts.length == 0;

      if (value instanceof List) {
        List indexesToDelete = new ArrayList();
        for (int i = 0; i < value.length; i++) {
          def item = value[i];
          boolean isTargetItem = updateDocByPath(item, newPathParts, params);
          if (params['opType'] == 'remove') {
            if (isTargetLevel) {
              if (item._id == params.newDocPart._id) {
                indexesToDelete.add(i);
              }
            } else if (isTargetItem && params.deleteParentArrayItem) {
              indexesToDelete.add(i);
            }
          }
          if (params['opType'] == 'update' && isTargetLevel) {
            value.set(i, params['newDocPart']);
          }
        }

        deleteByIndexes(value, indexesToDelete);

        return false;

      } else if (value instanceof Object) {
        boolean isTargetItem = updateDocByPath(value, newPathParts, params);
        
        if (isTargetLevel) {
          if (value._id == params.newDocPart._id) {
            if (params['opType'] == 'remove') {
       
              doc[pathPart] = null;

              return true;
            }
            if (params['opType'] == 'update') {
              doc[pathPart] = params['newDocPart'];
            }
          }
        }

        return isTargetItem;
      }
    }

    return false;
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

      if (mappingSchema[pathPart].type === 'nested') {
        return {
          nested: {
            path: currentPath.join('.'),
            query,
          },
        };
      }
    }

    return query;
  };

  const query = buildQueryRecursively(
    [],
    fieldPath.split('.'),
    mappingSchema[index].mappings.properties
  );
  return query;
};

const syncDependentIndexField = async ({
  indexName,
  mappingSchema,
  fieldPath,
  deleteParentArrayItem,
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

  const fieldPathParts = fieldPath.split('.');

  await client.updateByQuery({
    index: indexName,
    body: {
      script: {
        source: UPDATE_DEPENDENCIES_SCRIPT,
        lang: 'painless',
        params: {
          newDocPart,
          pathParts: fieldPathParts,
          opType,
          deleteParentArrayItem,
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
        fields.map(field =>
          syncDependentIndexField({
            indexName: name,
            mappingSchema,
            fieldPath: field.path || field,
            deleteParentArrayItem: !!field.deleteParentArrayItem,
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
