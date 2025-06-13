using System.Collections.Generic;
using System.Linq;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace ParquetSharp.Dataset;

internal sealed class TypeComparer
    : IArrowTypeVisitor
        , IArrowTypeVisitor<Decimal128Type>
        , IArrowTypeVisitor<Decimal256Type>
        , IArrowTypeVisitor<DictionaryType>
        , IArrowTypeVisitor<DurationType>
        , IArrowTypeVisitor<FixedSizeBinaryType>
        , IArrowTypeVisitor<FixedSizeListType>
        , IArrowTypeVisitor<IntervalType>
        , IArrowTypeVisitor<ListType>
        , IArrowTypeVisitor<LargeListType>
        , IArrowTypeVisitor<ListViewType>
        , IArrowTypeVisitor<MapType>
        , IArrowTypeVisitor<StructType>
        , IArrowTypeVisitor<Time32Type>
        , IArrowTypeVisitor<Time64Type>
        , IArrowTypeVisitor<TimestampType>
        , IArrowTypeVisitor<UnionType>
{
    public TypeComparer(IArrowType expectedType)
    {
        _expectedType = expectedType;
    }

    public bool TypesMatch { get; private set; } = false;

    public void Visit(DictionaryType type)
    {
        if (_expectedType is DictionaryType expectedType)
        {
            var indexComparer = new TypeComparer(expectedType.IndexType);
            var valueComparer = new TypeComparer(expectedType.ValueType);
            type.IndexType.Accept(indexComparer);
            type.ValueType.Accept(valueComparer);
            TypesMatch = indexComparer.TypesMatch
                         && valueComparer.TypesMatch
                         && type.Ordered == expectedType.Ordered;
        }
        else
        {
            TypesMatch = false;
        }
    }

    public void Visit(FixedSizeBinaryType type)
    {
        TypesMatch = _expectedType is FixedSizeBinaryType expectedType && type.ByteWidth == expectedType.ByteWidth;
    }

    public void Visit(IntervalType type)
    {
        TypesMatch = _expectedType is IntervalType expectedType && type.Unit == expectedType.Unit;
    }

    public void Visit(ListType type)
    {
        if (_expectedType is ListType expectedType)
        {
            var valueComparer = new TypeComparer(expectedType.ValueDataType);
            type.ValueDataType.Accept(valueComparer);
            TypesMatch = valueComparer.TypesMatch;
        }
        else
        {
            TypesMatch = false;
        }
    }

    public void Visit(LargeListType type)
    {
        if (_expectedType is LargeListType expectedType)
        {
            var valueComparer = new TypeComparer(expectedType.ValueDataType);
            type.ValueDataType.Accept(valueComparer);
            TypesMatch = valueComparer.TypesMatch;
        }
        else
        {
            TypesMatch = false;
        }
    }

    public void Visit(StructType type)
    {
        TypesMatch = _expectedType is StructType expectedType && FieldsMatch(type.Fields, expectedType.Fields);
    }

    public void Visit(Time32Type type)
    {
        TypesMatch = _expectedType is Time32Type expectedType && type.Unit == expectedType.Unit;
    }

    public void Visit(Time64Type type)
    {
        TypesMatch = _expectedType is Time64Type expectedType && type.Unit == expectedType.Unit;
    }

    public void Visit(TimestampType type)
    {
        TypesMatch = _expectedType is TimestampType expectedType
                     && type.Unit == expectedType.Unit
                     && type.Timezone == expectedType.Timezone;
    }

    public void Visit(UnionType type)
    {
        TypesMatch = _expectedType is UnionType expectedType
                     && type.Mode == expectedType.Mode
                     && type.TypeIds.SequenceEqual(expectedType.TypeIds);
    }

    public void Visit(Decimal128Type type)
    {
        TypesMatch = _expectedType is Decimal128Type expectedType
                     && type.Precision == expectedType.Precision
                     && type.Scale == expectedType.Scale;
    }

    public void Visit(Decimal256Type type)
    {
        TypesMatch = _expectedType is Decimal256Type expectedType
                     && type.Precision == expectedType.Precision
                     && type.Scale == expectedType.Scale;
    }

    public void Visit(DurationType type)
    {
        TypesMatch = _expectedType is DurationType expectedType
                     && type.Unit == expectedType.Unit;
    }

    public void Visit(FixedSizeListType type)
    {
        if (_expectedType is FixedSizeListType expectedType && type.ListSize == expectedType.ListSize)
        {
            var valueComparer = new TypeComparer(expectedType.ValueDataType);
            type.ValueDataType.Accept(valueComparer);
            TypesMatch = valueComparer.TypesMatch;
        }
        else
        {
            TypesMatch = false;
        }
    }

    public void Visit(ListViewType type)
    {
        if (_expectedType is ListViewType expectedType)
        {
            var valueComparer = new TypeComparer(expectedType.ValueDataType);
            type.ValueDataType.Accept(valueComparer);
            TypesMatch = valueComparer.TypesMatch;
        }
        else
        {
            TypesMatch = false;
        }
    }

    public void Visit(MapType type)
    {
        if (_expectedType is MapType expectedType)
        {
            var keyComparer = new TypeComparer(expectedType.KeyField.DataType);
            var valueComparer = new TypeComparer(expectedType.ValueField.DataType);
            type.KeyField.DataType.Accept(keyComparer);
            type.ValueField.DataType.Accept(valueComparer);
            TypesMatch = keyComparer.TypesMatch
                         && valueComparer.TypesMatch
                         && type.KeySorted == expectedType.KeySorted;
        }
        else
        {
            TypesMatch = false;
        }
    }

    public void Visit(IArrowType type)
    {
        TypesMatch = type.TypeId == _expectedType.TypeId;
    }

    private static bool FieldsMatch(IReadOnlyList<Field> actualFields, IReadOnlyList<Field> expectedFields)
    {
        if (actualFields.Count != expectedFields.Count)
        {
            return false;
        }

        for (var fieldIdx = 0; fieldIdx < expectedFields.Count; ++fieldIdx)
        {
            var actualField = actualFields[fieldIdx];
            var expectedField = expectedFields[fieldIdx];
            if (actualField.Name != expectedField.Name)
            {
                return false;
            }

            if (actualField.IsNullable != expectedField.IsNullable)
            {
                return false;
            }

            var fieldTypeComparer = new TypeComparer(expectedField.DataType);
            actualField.DataType.Accept(fieldTypeComparer);
            if (!fieldTypeComparer.TypesMatch)
            {
                return false;
            }
        }

        return true;
    }

    private readonly IArrowType _expectedType;
}
