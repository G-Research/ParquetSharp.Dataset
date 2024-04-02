using System;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace ParquetSharp.Dataset.Filter;

/// <summary>
/// Applies a filter mask to an array and creates a new array with only the filtered rows included.
/// </summary>
public class ArrayMaskApplier :
    IArrowArrayVisitor<UInt8Array>
    , IArrowArrayVisitor<UInt16Array>
    , IArrowArrayVisitor<UInt32Array>
    , IArrowArrayVisitor<UInt64Array>
    , IArrowArrayVisitor<Int8Array>
    , IArrowArrayVisitor<Int16Array>
    , IArrowArrayVisitor<Int32Array>
    , IArrowArrayVisitor<Int64Array>
    , IArrowArrayVisitor<HalfFloatArray>
    , IArrowArrayVisitor<FloatArray>
    , IArrowArrayVisitor<DoubleArray>
    , IArrowArrayVisitor<BooleanArray>
    , IArrowArrayVisitor<Date32Array>
    , IArrowArrayVisitor<Date64Array>
    , IArrowArrayVisitor<Time32Array>
    , IArrowArrayVisitor<Time64Array>
    , IArrowArrayVisitor<TimestampArray>
    , IArrowArrayVisitor<Decimal128Array>
    , IArrowArrayVisitor<Decimal256Array>
    , IArrowArrayVisitor<StringArray>
    , IArrowArrayVisitor<BinaryArray>
    , IArrowArrayVisitor<FixedSizeBinaryArray>
    , IArrowArrayVisitor<NullArray>
    , IArrowArrayVisitor<DictionaryArray>
    , IArrowArrayVisitor<ListArray>
    , IArrowArrayVisitor<StructArray>
    , IArrowArrayVisitor<MapArray>
{
    public ArrayMaskApplier(FilterMask mask)
    {
        if (mask == null)
        {
            throw new ArgumentNullException(nameof(mask));
        }

        _mask = mask.Mask;
        _includedCount = mask.IncludedCount;
    }

    private ArrayMaskApplier(ReadOnlyMemory<byte> mask, int includedCount)
    {
        _mask = mask;
        _includedCount = includedCount;
    }

    public IArrowArray MaskedArray
    {
        get
        {
            if (_maskedArray == null)
            {
                throw new InvalidOperationException("Array to mask has not been visited");
            }

            return _maskedArray;
        }
    }

    public void Visit(UInt8Array array) => VisitPrimitiveArray<byte, UInt8Array>(array, arrayData => new UInt8Array(arrayData));

    public void Visit(UInt16Array array) => VisitPrimitiveArray<ushort, UInt16Array>(array, arrayData => new UInt16Array(arrayData));

    public void Visit(UInt32Array array) => VisitPrimitiveArray<uint, UInt32Array>(array, arrayData => new UInt32Array(arrayData));

    public void Visit(UInt64Array array) => VisitPrimitiveArray<ulong, UInt64Array>(array, arrayData => new UInt64Array(arrayData));

    public void Visit(Int8Array array) => VisitPrimitiveArray<sbyte, Int8Array>(array, arrayData => new Int8Array(arrayData));

    public void Visit(Int16Array array) => VisitPrimitiveArray<short, Int16Array>(array, arrayData => new Int16Array(arrayData));

    public void Visit(Int32Array array) => VisitPrimitiveArray<int, Int32Array>(array, arrayData => new Int32Array(arrayData));

    public void Visit(Int64Array array) => VisitPrimitiveArray<long, Int64Array>(array, arrayData => new Int64Array(arrayData));

    public void Visit(HalfFloatArray array) => VisitPrimitiveArray<Half, HalfFloatArray>(array, arrayData => new HalfFloatArray(arrayData));

    public void Visit(FloatArray array) => VisitPrimitiveArray<float, FloatArray>(array, arrayData => new FloatArray(arrayData));

    public void Visit(DoubleArray array) => VisitPrimitiveArray<double, DoubleArray>(array, arrayData => new DoubleArray(arrayData));

    public void Visit(Date32Array array) => VisitPrimitiveArray<int, Date32Array>(array, arrayData => new Date32Array(arrayData));

    public void Visit(Date64Array array) => VisitPrimitiveArray<long, Date64Array>(array, arrayData => new Date64Array(arrayData));

    public void Visit(Time32Array array) => VisitPrimitiveArray<int, Time32Array>(array, arrayData => new Time32Array(arrayData));

    public void Visit(Time64Array array) => VisitPrimitiveArray<long, Time64Array>(array, arrayData => new Time64Array(arrayData));

    public void Visit(TimestampArray array) => VisitPrimitiveArray<long, TimestampArray>(array, arrayData => new TimestampArray(arrayData));

    public void Visit(Decimal128Array array) => VisitFixedSizeBinaryArray<Decimal128Array>(array, arrayData => new Decimal128Array(arrayData));

    public void Visit(Decimal256Array array) => VisitFixedSizeBinaryArray<Decimal256Array>(array, arrayData => new Decimal256Array(arrayData));

    public void Visit(StringArray array) => VisitBinaryArray<StringArray>(array, arrayData => new StringArray(arrayData));

    public void Visit(FixedSizeBinaryArray array) => VisitFixedSizeBinaryArray<FixedSizeBinaryArray>(array, arrayData => new FixedSizeBinaryArray(arrayData));

    public void Visit(BinaryArray array) => VisitBinaryArray<BinaryArray>(array, arrayData => new BinaryArray(arrayData));

    public void Visit(NullArray array)
    {
        _maskedArray = new NullArray(_includedCount);
    }

    public void Visit(BooleanArray array)
    {
        var builder = new BooleanArray.Builder();
        builder.Reserve(_includedCount);

        for (var i = 0; i < array.Length; ++i)
        {
            if (BitUtility.GetBit(_mask.Span, i))
            {
                builder.NullableAppend(array.GetValue(i));
            }
        }

        _maskedArray = builder.Build();
    }

    public void Visit(DictionaryArray array)
    {
        var indicesVisitor = new ArrayMaskApplier(_mask, _includedCount);
        array.Indices.Accept(indicesVisitor);
        var indicesArray = indicesVisitor.MaskedArray;
        _maskedArray = new DictionaryArray((DictionaryType)array.Data.DataType, indicesArray, array.Dictionary);
    }

    public void Visit(ListArray array)
    {
        // MapArray doesn't override Accept as of Arrow 15.0.2,
        // see https://github.com/apache/arrow/issues/40788
        if (array is MapArray mapArray)
        {
            Visit(mapArray);
        }
        else
        {
            VisitListArray(array, arrayData => new ListArray(arrayData));
        }
    }

    public void Visit(MapArray array) => VisitListArray(array, arrayData => new MapArray(arrayData));

    private void VisitListArray(ListArray array, Func<ArrayData, ListArray> arrayConstructor)
    {
        var valuesMaskBuilder = new ArrowBuffer.BitmapBuilder(array.Values.Length);
        var validityBuffer = new ArrowBuffer.BitmapBuilder(_includedCount);
        var valueOffsetsBuilder = new ArrowBuffer.Builder<int>(_includedCount + 1);

        valueOffsetsBuilder.Append(0);

        var outputValuesOffset = 0;
        var inputValuesOffset = array.ValueOffsets[0];
        if (inputValuesOffset > 0)
        {
            valuesMaskBuilder.AppendRange(false, inputValuesOffset);
        }

        for (var i = 0; i < array.Length; ++i)
        {
            var included = BitUtility.GetBit(_mask.Span, i);
            var nextOffset = array.ValueOffsets[i + 1];
            var length = nextOffset - inputValuesOffset;
            if (length > 0)
            {
                valuesMaskBuilder.AppendRange(included, length);
            }

            inputValuesOffset = nextOffset;

            if (included)
            {
                validityBuffer.Append(array.IsValid(i));
                outputValuesOffset += length;
                valueOffsetsBuilder.Append(outputValuesOffset);
            }
        }

        var valuesMaskApplier = new ArrayMaskApplier(valuesMaskBuilder.Build().Memory, valuesMaskBuilder.SetBitCount);
        array.Values.Accept(valuesMaskApplier);
        var valuesArray = valuesMaskApplier.MaskedArray;

        var arrayData = new ArrayData(array.Data.DataType, _includedCount, validityBuffer.UnsetBitCount, 0,
            new[] { validityBuffer.Build(), valueOffsetsBuilder.Build() }, new[] { valuesArray.Data });
        _maskedArray = arrayConstructor(arrayData);
    }

    public void Visit(StructArray array)
    {
        var validityBuffer = new ArrowBuffer.BitmapBuilder(_includedCount);

        for (var i = 0; i < array.Length; ++i)
        {
            if (BitUtility.GetBit(_mask.Span, i))
            {
                validityBuffer.Append(array.IsValid(i));
            }
        }

        var fields = new IArrowArray[array.Fields.Count];
        for (var fieldIdx = 0; fieldIdx < array.Fields.Count; ++fieldIdx)
        {
            var maskApplier = new ArrayMaskApplier(_mask, _includedCount);
            array.Fields[fieldIdx].Accept(maskApplier);
            fields[fieldIdx] = maskApplier.MaskedArray;
        }

        _maskedArray = new StructArray(
            array.Data.DataType, _includedCount, fields, validityBuffer.Build(),
            nullCount: validityBuffer.UnsetBitCount, offset: 0);
    }

    public void Visit(IArrowArray array)
    {
        throw new NotImplementedException($"Filtering an array of type {array.Data.DataType} is not implemented");
    }

    private void VisitPrimitiveArray<T, TArray>(TArray array, Func<ArrayData, TArray> arrayConstructor)
        where T : struct
        where TArray : PrimitiveArray<T>
    {
        var valueBuffer = new ArrowBuffer.Builder<T>(_includedCount);
        valueBuffer.Resize(_includedCount);

        var sourceValues = array.Values;
        var outputValues = valueBuffer.Span;

        var outputIndex = 0;
        for (var i = 0; i < array.Length; ++i)
        {
            if (BitUtility.GetBit(_mask.Span, i))
            {
                outputValues[outputIndex++] = sourceValues[i];
            }
        }

        int nullCount;
        ArrowBuffer validityBuffer;

        if (array.NullCount == 0)
        {
            nullCount = 0;
            validityBuffer = ArrowBuffer.Empty;
        }
        else
        {
            var validityBuilder = new ArrowBuffer.BitmapBuilder(_includedCount);
            for (var i = 0; i < array.Length; ++i)
            {
                if (BitUtility.GetBit(_mask.Span, i))
                {
                    validityBuilder.Append(array.IsValid(i));
                }
            }

            nullCount = validityBuilder.UnsetBitCount;
            validityBuffer = validityBuilder.Build();
        }

        var arrayData = new ArrayData(
            array.Data.DataType, _includedCount, nullCount, 0,
            new[] { validityBuffer, valueBuffer.Build() });
        _maskedArray = arrayConstructor(arrayData);
    }

    private void VisitFixedSizeBinaryArray<TArray>(TArray array, Func<ArrayData, TArray> arrayConstructor)
        where TArray : FixedSizeBinaryArray
    {
        var size = ((FixedSizeBinaryType)array.Data.DataType).ByteWidth;
        var valueBuffer = new ArrowBuffer.Builder<byte>(_includedCount * size);
        var validityBuffer = new ArrowBuffer.BitmapBuilder(_includedCount);

        var sourceValues = array.ValueBuffer.Span;
        var offset = array.Offset;

        for (var i = 0; i < array.Length; ++i)
        {
            if (BitUtility.GetBit(_mask.Span, i))
            {
                valueBuffer.Append(sourceValues.Slice((offset + i) * size, size));
                validityBuffer.Append(array.IsValid(i));
            }
        }

        var arrayData = new ArrayData(
            array.Data.DataType, _includedCount, validityBuffer.UnsetBitCount, 0,
            new[] { validityBuffer.Build(), valueBuffer.Build() });
        _maskedArray = arrayConstructor(arrayData);
    }

    private void VisitBinaryArray<TArray>(TArray array, Func<ArrayData, TArray> arrayConstructor)
        where TArray : BinaryArray
    {
        var dataBuffer = new ArrowBuffer.Builder<byte>();
        var valueOffsetsBuffer = new ArrowBuffer.Builder<int>(_includedCount);
        var validityBuffer = new ArrowBuffer.BitmapBuilder(_includedCount);

        var sourceOffsets = array.ValueOffsets;
        var sourceValues = array.Values;

        var offset = 0;
        valueOffsetsBuffer.Append(0);
        for (var i = 0; i < array.Length; ++i)
        {
            if (BitUtility.GetBit(_mask.Span, i))
            {
                var isValid = array.IsValid(i);
                if (isValid)
                {
                    var sourceOffset = sourceOffsets[i];
                    var length = sourceOffsets[i + 1] - sourceOffset;
                    dataBuffer.Append(sourceValues.Slice(sourceOffset, length));
                    offset += length;
                }

                validityBuffer.Append(isValid);
                valueOffsetsBuffer.Append(offset);
            }
        }

        var arrayData = new ArrayData(
            array.Data.DataType, _includedCount, validityBuffer.UnsetBitCount, 0,
            new[] { validityBuffer.Build(), valueOffsetsBuffer.Build(), dataBuffer.Build() });
        _maskedArray = arrayConstructor(arrayData);
    }

    private readonly ReadOnlyMemory<byte> _mask;
    private readonly int _includedCount;
    private IArrowArray? _maskedArray = null;
}
