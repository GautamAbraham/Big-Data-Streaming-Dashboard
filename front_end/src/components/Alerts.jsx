function Alerts({ messages }) {
  return (
    <div className="fixed top-20 right-4 space-y-2 z-50">
      {messages.map((msg, index) => (
        <div
          key={index}
          className="bg-red-600 text-white px-4 py-2 rounded shadow fade-out"
        >
          {msg}
        </div>
      ))}
    </div>
  );
}

export default Alerts;
