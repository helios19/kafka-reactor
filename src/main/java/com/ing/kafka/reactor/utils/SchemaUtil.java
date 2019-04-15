package com.ing.kafka.reactor.utils;

import avro.shaded.com.google.common.base.Objects;
import org.apache.avro.Schema;

import java.util.Iterator;

/**
 * Created by helios on 11/04/19.
 */
public class SchemaUtil {


    public static Schema mergeOrUnion(Iterable<Schema> schemas) {
        Iterator<Schema> iter = schemas.iterator();
        if (!iter.hasNext()) {
            return null;
        }
        Schema result = iter.next();
        while (iter.hasNext()) {
            result = mergeOrUnion(result, iter.next());
        }
        return result;
    }

    private static Schema mergeOrUnion(Schema left, Schema right) {
        Schema merged = mergeOnly(left, right);
        if (merged != null) {
            return merged;
        }
        return null;
//        return union(left, right);
    }

    /**
     * Merges two {@link Schema} instances or returns {@code null}.
     * <p>
     * The two schemas are merged if they are the same type. Records are merged
     * if the two records have the same name or have no names but have a
     * significant number of shared fields.
     * <p>
     * @see {@link #mergeOrUnion} to return a union when a merge is not possible.
     *
     * @param left a {@code Schema}
     * @param right a {@code Schema}
     * @return a merged {@code Schema} or {@code null} if merging is not possible
     */
    private static Schema mergeOnly(Schema left, Schema right) {
        if (Objects.equal(left, right)) {
            return left;
        }

        // handle primitive type promotion; doesn't promote integers to floats
        switch (left.getType()) {
            case INT:
                if (right.getType() == Schema.Type.LONG) {
                    return right;
                }
                break;
            case LONG:
                if (right.getType() == Schema.Type.INT) {
                    return left;
                }
                break;
            case FLOAT:
                if (right.getType() == Schema.Type.DOUBLE) {
                    return right;
                }
                break;
            case DOUBLE:
                if (right.getType() == Schema.Type.FLOAT) {
                    return left;
                }
        }

        // any other cases where the types don't match must be combined by a union
        if (left.getType() != right.getType()) {
            return null;
        }

        switch (left.getType()) {
//            case UNION:
//                return union(left, right);
//            case RECORD:
//                if (left.getName() == null && right.getName() == null &&
//                        fieldSimilarity(left, right) < SIMILARITY_THRESH) {
//                    return null;
//                } else if (!Objects.equal(left.getName(), right.getName())) {
//                    return null;
//                }
//
//                Schema combinedRecord = Schema.createRecord(
//                        coalesce(left.getName(), right.getName()),
//                        coalesce(left.getDoc(), right.getDoc()),
//                        coalesce(left.getNamespace(), right.getNamespace()),
//                        false
//                );
//                combinedRecord.setFields(mergeFields(left, right));
//
//                return combinedRecord;
//
//            case MAP:
//                return Schema.createMap(
//                        mergeOrUnion(left.getValueType(), right.getValueType()));
//
//            case ARRAY:
//                return Schema.createArray(
//                        mergeOrUnion(left.getElementType(), right.getElementType()));
//
//            case ENUM:
//                if (!Objects.equal(left.getName(), right.getName())) {
//                    return null;
//                }
//                Set<String> symbols = Sets.newLinkedHashSet();
//                symbols.addAll(left.getEnumSymbols());
//                symbols.addAll(right.getEnumSymbols());
//                return Schema.createEnum(
//                        left.getName(),
//                        coalesce(left.getDoc(), right.getDoc()),
//                        coalesce(left.getNamespace(), right.getNamespace()),
//                        ImmutableList.copyOf(symbols)
//                );

            default:
                // all primitives are handled before the switch by the equality check.
                // schemas that reach this point are not primitives and also not any of
                // the above known types.
                throw new UnsupportedOperationException(
                        "Unknown schema type: " + left.getType());
        }
    }



}
