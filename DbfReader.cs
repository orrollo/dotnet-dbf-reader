using System;
using System.IO;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace DbfReader
{
	public static class DbfHelper
	{
		public static T ToStructure<T>(this byte[] bytes) where T : struct
		{
			var handle = GCHandle.Alloc(bytes, GCHandleType.Pinned);
			var stuff = (T)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(T));
			handle.Free();
			return stuff;
		}		
	}

	public class DbfReader
	{
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

		[StructLayout(LayoutKind.Explicit, Size = 32, Pack = 1)]
		protected struct DbfFieldStructure 
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

		protected enum DbfType
		{
			Binary = 'B',
			Character = 'C',
			Date = 'D',
			Float = 'F',
			Integer = 'I',
			Logical = 'L',
			Memo = 'M',
			Numeric = 'N',
			Double = 'O'
		}

		protected BinaryReader BinaryReader;
		protected DbfHeader Header;
		protected DbfField[] Fields;

		public DbfReader(Stream inputStream, Encoding encoding = null)
		{
			InitEncoding(encoding);
			InitInputStream(inputStream);
		}

		private void InitEncoding(Encoding encoding)
		{
			if (encoding == null) encoding = Encoding.ASCII;
			CharEncoding = encoding;
		}

		public DbfReader(string fileName, Encoding encoding = null)
		{
			InitEncoding(encoding);
			InitInputStream(File.OpenRead(fileName));
		}

		private void InitInputStream(Stream inputStream)
		{
			if (inputStream == null) throw new ArgumentException("Input stream is null");
			InitBinaryReader(inputStream);
			ReadHeader();
			ReadFields();
			var pos = Header.HeaderLength - BinaryReader.BaseStream.Position;
			if (pos > 0) BinaryReader.ReadBytes((int) pos);
		}

		private void ReadFields()
		{
			var bytes = new byte[32];
			var fields = new List<DbfField>();
			while (true)
			{
				bytes[0] = BinaryReader.ReadByte();
				if (bytes[0]==0x0d) break;
				BinaryReader.Read(bytes, 1, bytes.Length - 1);
				var field = bytes.ToStructure<DbfFieldStructure>();
				fields.Add(new DbfField(field));
			}
			Fields = fields.ToArray();
		}

		protected class DbfField
		{
			public string Name;
			public DbfType DbfType;
			public int FieldLength;
			public int Decimals;

			public DbfField(DbfFieldStructure field)
			{
				Name = field.Name;
				DbfType = (DbfType) field.TypeChar;
				FieldLength = field.FieldLength;
				var isChar = DbfType == DbfType.Character;
				if (isChar) FieldLength += 256*field.Decimals;
				Decimals = isChar ? 0 : field.Decimals;
			}
		}

		private void ReadHeader()
		{
			Header = BinaryReader.ReadBytes(32).ToStructure<DbfHeader>();
			if ((Header.Signature & 7) > 3) throw new ArgumentException("DBF file has unsuppurted version");
		}

		private void InitBinaryReader(Stream inputStream)
		{
			BinaryReader = new BinaryReader(inputStream, CharEncoding);
		}

		public Encoding CharEncoding { get; protected set; }
	}
}
