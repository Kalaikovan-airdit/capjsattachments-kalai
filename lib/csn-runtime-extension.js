const cds = require('@sap/cds')

function collectAttachments(ent, resultSet = [], path = []) {
  if (!ent.compositions) return resultSet
  for (const ele of Object.keys(ent.compositions)) {
    const target = ent.compositions[ele]._target
    const newPath = [...path, ele]
    if (target?.["@_is_media_data"]) {
      resultSet.push(newPath)
    }
    if (target && target !== ent) collectAttachments(target, resultSet, newPath)
  }
  return resultSet
}

// Check if the property already exists to prevent crashes
if (!Object.prototype.hasOwnProperty.call(cds.builtin.classes.entity.prototype, '_attachments')) {
  
  Object.defineProperty(cds.builtin.classes.entity.prototype, '_attachments', {
    configurable: true, // <--- CRITICAL: Allows redefinition/extension without crashing
    enumerable: false,
    get() {
      const entity = this;
      return {
        get hasAttachmentsComposition() {
          return entity.compositions && 
                 Object.keys(entity.compositions).some(ele => 
                   entity.compositions[ele]._target?.["@_is_media_data"] || 
                   entity.compositions[ele]._target?._attachments?.hasAttachmentsComposition
                 );
        },
        get attachmentCompositions() {
          // Ensure collectAttachments is imported/available in your lib
          return collectAttachments(entity); 
        },
        get isAttachmentsEntity() {
          return !!entity?.["@_is_media_data"];
        }
      };
    },
  });
}
