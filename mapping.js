export default {
    dynamic_templates: [{
        notanalyzed: {
            match: '*',
            match_mapping_type: 'string',
            mapping: {
                type: 'string',
                index: 'not_analyzed'
            }
        }
    }],
    properties: {
        tittel: {
            type: 'string',
            index: 'analyzed'
        }
    }
};