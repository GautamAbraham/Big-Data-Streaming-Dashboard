import { MenuIcon } from "../assets/icons";

function Navbar({ toggleSidebar }) {
  return (
    <nav className="bg-white shadow flex items-center">
      <button
        onClick={toggleSidebar}
        className="p-2 ml-4 rounded hover:bg-gray-200 focus:outline-none"
      >
        <MenuIcon />
      </button>
      <div className="flex-grow text-center">
        <h1 className="text-xl font-semibold my-4">
          {"Radiation Tracker in Real-Time"}
        </h1>
      </div>
    </nav>
  );
}

export default Navbar;
