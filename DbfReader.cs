/*
 * 
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Leonid Koninin
 * 
 * Permission is hereby granted, free of charge, to any 
 * person obtaining a copy of this software and associated 
 * documentation files (the "Software"), to deal in the 
 * Software without restriction, including without limitation 
 * the rights to use, copy, modify, merge, publish, distribute, 
 * sublicense, and/or sell copies of the Software, and to 
 * permit persons to whom the Software is furnished to do 
 * so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall 
 * be included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, 
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. 
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE 
 * FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION 
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * -------------------------------------------------------------------------
 * source code of library located at:
 * http://github.com/orrollo/dotnet-dbf-reader
 * 
 * version 1.0 - may 2014 - initial
 * 
 */

using System;
using System.Globalization;
using System.IO;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace DotNetDbfReader
{
	/// <summary>
	/// all our exceptions are wrapped to this class
	/// </summary>
	public class DbfReaderException : ArgumentException
	{
		public DbfReaderException(string msg) : base(msg)
		{
			
		}

		public DbfReaderException(string msg, Exception e) : base(msg, e)
		{
			
		}
	}

	/// <summary>
	/// base class for reading. single class.
	/// </summary>
	public class DbfReader : IDisposable
	{
		/// <summary>
		/// internal dbf header structure
		/// </summary>
		[StructLayout(LayoutKind.Explicit, Size = 32, Pack = 1)]
		protected struct DbfHeader
		{
			[FieldOffset(0)]
			public byte Signature;
			[FieldOffset(1)]
			public byte Year;
			[FieldOffset(2)]
			public byte Month;
			[FieldOffset(3)]
			public byte Day;
			[FieldOffset(4)]
			public uint RecordCount;
			[FieldOffset(8)]
			public ushort HeaderLength;
			[FieldOffset(10)]
			public ushort RecordLength;
			[FieldOffset(12)]
			public ushort Reserved1;
			[FieldOffset(14)]
			public byte TransactionFlag;
			[FieldOffset(15)]
			public byte EncodedFlag;
			[FieldOffset(16)]
			public ulong Reserved2;
			[FieldOffset(24)]
			public ulong Reserved3;
			[FieldOffset(28)]
			public byte IndexFlag;
			[FieldOffset(29)]
			public byte Language;
			[FieldOffset(30)]
			public ushort Reserved4;
		}

		/// <summary>
		/// internal field definition structure
		/// </summary>
		[StructLayout(LayoutKind.Explicit, Size = 32, Pack = 1)]
		public struct DbfFieldStructure 
		{
			[FieldOffset(0)]
			[MarshalAs(UnmanagedType.ByValTStr, SizeConst = 11)]
			public string Name;
			[FieldOffset(11)]
			public byte TypeChar;
			[FieldOffset(12)]
			public uint Reserved1;
			[FieldOffset(16)]
			public byte FieldLength;
			[FieldOffset(17)]
			public byte Decimals;
		}

		protected BinaryReader BinaryReader;
		protected BinaryReader DbtReader;
		protected DbfHeader Header;
		protected Dictionary<string, int> FieldsIndex;
		protected object[] Record;
		protected int DataLength;

		/// <summary>
		/// create reader using input stream and encoding objects
		/// </summary>
		/// <param name="inputStream">stream to read</param>
		/// <param name="encoding">default encoding is ascii</param>
		public DbfReader(Stream inputStream, Encoding encoding = null)
		{
			InitReader(inputStream, null, encoding);
		}

		/// <summary>
		/// create reader using input stream, dbt (memo file) stream and encoding objects
		/// </summary>
		/// <param name="inputStream">stream to read</param>
		/// <param name="dbtStream">stream to read memo fields</param>
		/// <param name="encoding">default encoding is ascii</param>
		public DbfReader(Stream inputStream, Stream dbtStream, Encoding encoding = null)
		{
			InitReader(inputStream, dbtStream, encoding);
		}

		/// <summary>
		/// create reader using input file name and encoding
		/// </summary>
		/// <param name="fileName">input file path</param>
		/// <param name="encoding">default encoding is ascii</param>
		public DbfReader(string fileName, Encoding encoding = null)
		{
			InitReader(OpenFileWithSharing(fileName), null, encoding);
		}

		private static FileStream OpenFileWithSharing(string fileName)
		{
			return File.Open(fileName, FileMode.Open, FileAccess.Read, FileShare.Read);
		}

		/// <summary>
		/// create reader using input file name, dbt (memo) file name and encoding
		/// </summary>
		/// <param name="fileName">input file path</param>
		/// <param name="dbtFileName">dbt (memo) file path</param>
		/// <param name="encoding">default encoding is ascii</param>
		public DbfReader(string fileName, string dbtFileName, Encoding encoding = null)
		{
			InitReader(OpenFileWithSharing(fileName), OpenFileWithSharing(dbtFileName), encoding);
		}

		private void InitReader(Stream inputStream, Stream dbtStream, Encoding encoding)
		{
			CharEncoding = encoding ?? Encoding.ASCII;
			if (inputStream == null) throw new DbfReaderException("Input stream is null");
			BinaryReader = new BinaryReader(inputStream, CharEncoding);
			if (dbtStream != null) DbtReader = new BinaryReader(dbtStream, CharEncoding);
			ReadHeader();
			ReadFields();
			var pos = Header.HeaderLength - BinaryReader.BaseStream.Position;
			if (pos > 0) BinaryReader.ReadBytes((int) pos);
		}

		private void ReadFields()
		{
			var bytes = new byte[32];
			var fields = new List<DbfField>();
			var dataSize = 1;
			while (true)
			{
				bytes[0] = BinaryReader.ReadByte();
				if (bytes[0]==0x0d) break;
				BinaryReader.Read(bytes, 1, bytes.Length - 1);
				var field = ToStructure<DbfFieldStructure>(bytes);
				fields.Add(new DbfField(field));
				dataSize += field.FieldLength;
			}
			Fields = fields.ToArray();
			FieldsIndex = CreateFieldsIndex();
			DataLength = Math.Max(dataSize, Header.RecordLength);
		}

		public bool Read()
		{
			PrepareRecord();

		    if (!SkipDeleted())
			{
				Record = null;
				return false;
			}

			// minus 1 because we are after the 'deleted' sign byte
			var pos = BinaryReader.BaseStream.Position - 1;
			var data = new byte[DataLength];

			for (var fieldIndex = 0; fieldIndex < Fields.Length; fieldIndex++)
			{
				var length = Fields[fieldIndex].FieldLength;
				if (BinaryReader.Read(data, 0, length) != length) return false;
				Record[fieldIndex] = DecodeBytes(data, fieldIndex);
			}

			var delta = DataLength - (BinaryReader.BaseStream.Position - pos);
			if (delta > 0) BinaryReader.ReadBytes((int) delta);
			return true;
		}

		private object DecodeBytes(byte[] data, int fieldIndex)
		{
			var field = Fields[fieldIndex];
			try
			{
				var type = field.DbfType;
				if (type == DbfType.Double || type == DbfType.Numeric)
					return DecodeFloatPoint(data, field, type);
				if (type == DbfType.Date)
					return DecodeDate(data);
				if (type == DbfType.Logical)
					return DecodeLogical(data);
				if (type == DbfType.Memo)
					return DecodeMemo(data, fieldIndex);
				return CharEncoding.GetString(data, 0, field.FieldLength).TrimEnd();
			}
			catch (Exception e)
			{
				var msg = String.Format("Unable to decode field [{0}], value <{1}>", field.Name,
					CharEncoding.GetString(data, 0, field.FieldLength));
				throw new DbfReaderException(msg, e);
			}
		}

		private object DecodeMemo(byte[] data, int fieldIndex)
		{
			if (DbtReader == null) throw new DbfReaderException("Unable to read memo file");
			var str = CharEncoding.GetString(data, 0, Fields[fieldIndex].FieldLength);
			if (string.IsNullOrWhiteSpace(str)) return null;
			long index;
			if (!long.TryParse(str.Trim(), out index)) return null;
			// reading memo file
			DbtReader.BaseStream.Seek((index + 1) << 9, SeekOrigin.Begin);
			
			var ms = new MemoryStream();
			var xCnt = 0;
			// read until [0x1a, 0x1a] in file
			while (xCnt < 2)
			{
				var bt = DbtReader.ReadByte();
				ms.WriteByte(bt);
				xCnt = bt == 0x1a ? xCnt + 1 : 0;
			}
			var memo = ms.ToArray();
			var retString = CharEncoding.GetString(new byte[] {0x8d, 0x0a});
			return CharEncoding.GetString(memo, 0, memo.Length - 2).Replace(retString, string.Empty);
		}

		private static object DecodeLogical(byte[] data)
		{
			var value = data[0];
			object ret = null;
			if (value != '?') ret = value == 't' || value == 'T' || value == 'y' || value == 'Y';
			return ret;
		}

		private object DecodeDate(byte[] data)
		{
			var strYear = CharEncoding.GetString(data, 0, 4);
			var strMonth = CharEncoding.GetString(data, 4, 2);
			var strDay = CharEncoding.GetString(data, 6, 2);

			object ret = null;
			int year, month, day;
			if (Int32.TryParse(strYear, out year) && Int32.TryParse(strMonth, out month)
			    && Int32.TryParse(strDay, out day))
			{
				ret = new DateTime(year, month, day);
			}
			return ret;
		}

		const NumberStyles Styles = NumberStyles.AllowLeadingWhite | NumberStyles.Float;

		private object DecodeFloatPoint(byte[] data, DbfField field, DbfType type)
		{
			object ret = null;
			var str = CharEncoding.GetString(data, 0, field.FieldLength);
			if (str.Length != 0)
			{
				var last = str[str.Length - 1];
				if (last != ' ' && last != '?')
				{
					var culture = CultureInfo.InvariantCulture;
					str = str.Replace(',', '.');
					ret = type == DbfType.Double
						      ? (object) Double.Parse(str, Styles, culture)
							  : Decimal.Parse(str, Styles, culture);
				}
			}
			return ret;
		}

		private bool IsEof()
		{
			return BinaryReader.BaseStream.Position == BinaryReader.BaseStream.Length;
		}

		private void PrepareRecord()
		{
			if (Record == null)
				Record = new object[Fields.Length];
			else
				for (int i = 0; i < Record.Length; i++) Record[i] = null;
		}

		private bool SkipDeleted()
		{
			if (IsEof()) return false;
			var sign = BinaryReader.ReadByte();
			var data = new byte[DataLength - 1];

			while (sign == '*')
			{
				BinaryReader.Read(data, 0, DataLength - 1);
				if (IsEof()) return false;
				sign = BinaryReader.ReadByte();
			}
			return true;
		}

		private Dictionary<string, int> CreateFieldsIndex()
		{
			var ret = new Dictionary<string, int>();
			for (var index = 0; index < Fields.Length; index++) ret[Fields[index].Name] = index;
			return ret;
		}

		private void ReadHeader()
		{
			Header = ToStructure<DbfHeader>(BinaryReader.ReadBytes(32));
			if ((Header.Signature & 7) > 3) throw new DbfReaderException("DBF file has unsuppurted version");
		}

		public Encoding CharEncoding { get; protected set; }

		public DbfField[] Fields { get; protected set; }

		public uint RecordCount
		{
			get { return Header.RecordCount; }
		}

		public object this[int ordinal]
		{
			get { return GetValue(ordinal); }
		}

		public object this[string name]
		{
			get { return GetValue(GetOrdinal(name)); }
		}

		public object[] GetValues()
		{
			if (Record == null) throw new DbfReaderException("Record not readed");
			return Record;
		}

		public object GetValue(int ordinal)
		{
			if (ordinal < 0 || ordinal >= Fields.Length) throw new DbfReaderException("Field not found");
			if (Record == null) throw new DbfReaderException("Record not readed");
			return Record[ordinal];
		}

		public DbfField GetField(int ordinal)
		{
			return ordinal > 0 && ordinal < Fields.Length ? Fields[ordinal] : null;
		}

		public DbfField GetField(string name)
		{
			return GetField(GetOrdinal(name));
		}

		public int GetOrdinal(string name)
		{
			FieldsIndex = FieldsIndex ?? CreateFieldsIndex();
			int idx = FieldsIndex.ContainsKey(name) ? FieldsIndex[name] : -1;
			return idx;
		}

		public void Dispose()
		{
			if (BinaryReader != null)
			{
				BinaryReader.Close();
				BinaryReader = null;
			}
		}

		public static T ToStructure<T>(byte[] bytes) where T : struct
		{
			var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
			var stuff = (T)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(T));
			handle.Free();
			return stuff;
		}
	}

	public class DbfField
	{
		private readonly string _name;
		private readonly DbfType _dbfType;
		private readonly int _fieldLength;
		private readonly int _decimals;

		public DbfField(DbfReader.DbfFieldStructure field)
		{
			_name = field.Name;
			_dbfType = (DbfType) field.TypeChar;
			_fieldLength = field.FieldLength;
			var isChar = _dbfType == DbfType.Character;
			if (isChar) _fieldLength += 256*field.Decimals;
			_decimals = isChar ? 0 : field.Decimals;
		}

		public string Name
		{
			get { return _name; }
		}

		public DbfType DbfType
		{
			get { return _dbfType; }
		}

		public int FieldLength
		{
			get { return _fieldLength; }
		}

		public int Decimals
		{
			get { return _decimals; }
		}
	}

	public enum DbfType
	{
		Binary = 'B',		// not supported now
		Character = 'C',
		Date = 'D',
		Float = 'F',
		Integer = 'I',		// not supported now
		Logical = 'L',
		Memo = 'M',			// not supported now
		Numeric = 'N',
		Double = 'O'
	}
}
